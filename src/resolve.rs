use std::fs::create_dir_all;
use std::path::{Component, Path, PathBuf};

use sqlx_sqlite_conn_mgr::SqliteDatabaseConfig;
use sqlx_sqlite_toolkit::DatabaseWrapper;
use tauri::{AppHandle, Manager, Runtime};

use crate::Error;

/// Connect to a SQLite database via the connection manager, resolving
/// the path relative to the app config directory.
///
/// This is the Tauri-specific connection method that resolves relative paths
/// before delegating to the toolkit's `DatabaseWrapper::connect()`.
pub async fn connect<R: Runtime>(
   path: &str,
   app: &AppHandle<R>,
   custom_config: Option<SqliteDatabaseConfig>,
) -> Result<DatabaseWrapper, Error> {
   let abs_path = resolve_database_path(path, app)?;
   Ok(DatabaseWrapper::connect(&abs_path, custom_config).await?)
}

/// Resolve database file path relative to app config directory.
///
/// Paths are joined to `app_config_dir()` (e.g., `Library/Application Support/${bundleIdentifier}`
/// on iOS). Special paths like `:memory:` are passed through unchanged.
///
/// Returns `Err(Error::PathTraversal)` if the path attempts to escape the app config directory
/// via absolute paths, `..` segments, or null bytes.
pub fn resolve_database_path<R: Runtime>(path: &str, app: &AppHandle<R>) -> Result<PathBuf, Error> {
   let app_path = app
      .path()
      .app_config_dir()
      .map_err(|_| Error::InvalidPath("No app config path found".to_string()))?;

   create_dir_all(&app_path)?;

   validate_and_resolve(path, &app_path)
}

/// Validate a user-supplied path and resolve it against a base directory.
///
/// In-memory database paths are passed through unchanged. All other paths are validated
/// to ensure they cannot escape the base directory.
fn validate_and_resolve(path: &str, base: &Path) -> Result<PathBuf, Error> {
   // Pass through in-memory database paths unchanged — they don't touch the filesystem.
   // Matches the same patterns as `is_memory_database` in sqlx-sqlite-conn-mgr.
   if is_memory_path(path) {
      return Ok(PathBuf::from(path));
   }

   // Reject null bytes — these can truncate paths in C-level filesystem calls
   if path.contains('\0') {
      return Err(Error::PathTraversal("path contains null byte".to_string()));
   }

   let rel = Path::new(path);

   // Reject absolute paths — PathBuf::join replaces the base when given an absolute path
   if rel.is_absolute() {
      return Err(Error::PathTraversal(
         "absolute paths are not allowed".to_string(),
      ));
   }

   // Reject parent directory components — prevents escaping the base via `../`
   for component in rel.components() {
      if matches!(component, Component::ParentDir) {
         return Err(Error::PathTraversal(
            "parent directory references are not allowed".to_string(),
         ));
      }
   }

   // Join and canonicalize to verify containment. The parent directory is canonicalized
   // because the file may not exist yet.
   let joined = base.join(rel);
   let canonical_base = base
      .canonicalize()
      .map_err(|e| Error::InvalidPath(format!("cannot canonicalize base path: {e}")))?;

   let canonical_resolved = if joined.exists() {
      joined.canonicalize()
   } else {
      // Canonicalize the parent and re-append the file name
      joined
         .parent()
         .ok_or_else(|| Error::InvalidPath("path has no parent".to_string()))?
         .canonicalize()
         .map(|parent| parent.join(joined.file_name().unwrap_or_default()))
   }
   .map_err(|e| Error::InvalidPath(format!("cannot canonicalize path: {e}")))?;

   if !canonical_resolved.starts_with(&canonical_base) {
      return Err(Error::PathTraversal(
         "resolved path escapes the base directory".to_string(),
      ));
   }

   // Return the original (non-canonicalized) joined path for consistency with how the
   // rest of the codebase references database paths.
   Ok(joined)
}

/// Check if a path string represents an in-memory SQLite database.
///
/// Matches the same patterns as `is_memory_database` in `sqlx-sqlite-conn-mgr`:
/// `:memory:`, `file::memory:*` URIs, and `mode=memory` query parameters.
fn is_memory_path(path: &str) -> bool {
   path == ":memory:"
      || path.starts_with("file::memory:")
      || (path.starts_with("file:") && path.contains("mode=memory"))
}

#[cfg(test)]
mod tests {
   use super::*;
   use std::fs;

   /// Helper that creates a temporary base directory for testing.
   fn make_temp_base() -> PathBuf {
      let dir = std::env::temp_dir().join(format!("tauri_sqlite_test_{}", std::process::id()));
      fs::create_dir_all(&dir).unwrap();
      dir
   }

   #[test]
   fn test_simple_filename() {
      let base = make_temp_base();
      let result = validate_and_resolve("mydb.db", &base).unwrap();
      assert_eq!(result, base.join("mydb.db"));
   }

   #[test]
   fn test_subdirectory_path() {
      let base = make_temp_base();
      // Create the subdirectory so canonicalize can resolve it
      fs::create_dir_all(base.join("subdir")).unwrap();
      let result = validate_and_resolve("subdir/mydb.db", &base).unwrap();
      assert_eq!(result, base.join("subdir/mydb.db"));
   }

   #[test]
   fn test_memory_passthrough() {
      let base = make_temp_base();
      assert_eq!(
         validate_and_resolve(":memory:", &base).unwrap(),
         PathBuf::from(":memory:"),
      );
   }

   #[test]
   fn test_file_memory_uri_passthrough() {
      let base = make_temp_base();
      assert_eq!(
         validate_and_resolve("file::memory:?cache=shared", &base).unwrap(),
         PathBuf::from("file::memory:?cache=shared"),
      );
   }

   #[test]
   fn test_mode_memory_passthrough() {
      let base = make_temp_base();
      assert_eq!(
         validate_and_resolve("file:test?mode=memory", &base).unwrap(),
         PathBuf::from("file:test?mode=memory"),
      );
   }

   #[test]
   fn test_rejects_parent_traversal() {
      let base = make_temp_base();
      let err = validate_and_resolve("../../../etc/passwd", &base).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_absolute_path() {
      let base = make_temp_base();
      let err = validate_and_resolve("/etc/passwd", &base).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_embedded_traversal() {
      let base = make_temp_base();
      let err = validate_and_resolve("foo/../../bar", &base).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_null_byte() {
      let base = make_temp_base();
      let err = validate_and_resolve("path\0evil", &base).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_non_uri_mode_memory() {
      let base = make_temp_base();
      // A bare filename containing "mode=memory" is not a valid SQLite URI —
      // it should go through normal path validation, not be passed through.
      let result = validate_and_resolve("evil.db?mode=memory", &base).unwrap();
      assert_eq!(result, base.join("evil.db?mode=memory"));
   }
}
