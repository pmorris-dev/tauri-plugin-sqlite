//! Transaction management for interruptible transactions

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sqlx::{Column, Row};
use sqlx_sqlite_conn_mgr::{AttachedWriteGuard, WriteGuard};
use tokio::sync::{Mutex, RwLock};
use tokio::task::AbortHandle;
use tracing::{debug, warn};

#[cfg(feature = "observer")]
use sqlx_sqlite_observer::ObservableWriteGuard;

use crate::wrapper::WriterGuard;
use crate::{Error, Result, WriteQueryResult};

/// Wrapper around WriteGuard, ObservableWriteGuard, or AttachedWriteGuard
/// to unify transaction handling.
pub enum TransactionWriter {
   Regular(WriteGuard),
   Attached(AttachedWriteGuard),
   #[cfg(feature = "observer")]
   Observable(ObservableWriteGuard),
}

impl TransactionWriter {
   /// Execute a query on either writer type
   pub async fn execute_query<'a>(
      &mut self,
      query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
   ) -> Result<sqlx::sqlite::SqliteQueryResult> {
      match self {
         Self::Regular(w) => query.execute(&mut **w).await.map_err(Into::into),
         Self::Attached(w) => query.execute(&mut **w).await.map_err(Into::into),
         #[cfg(feature = "observer")]
         Self::Observable(w) => query.execute(&mut **w).await.map_err(Into::into),
      }
   }

   /// Fetch all rows from either writer type
   pub async fn fetch_all<'a>(
      &mut self,
      query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
   ) -> Result<Vec<sqlx::sqlite::SqliteRow>> {
      match self {
         Self::Regular(w) => query.fetch_all(&mut **w).await.map_err(Into::into),
         Self::Attached(w) => query.fetch_all(&mut **w).await.map_err(Into::into),
         #[cfg(feature = "observer")]
         Self::Observable(w) => query.fetch_all(&mut **w).await.map_err(Into::into),
      }
   }

   /// Begin an immediate transaction
   pub async fn begin_immediate(&mut self) -> Result<()> {
      self.execute_query(sqlx::query("BEGIN IMMEDIATE")).await?;
      Ok(())
   }

   /// Commit the current transaction
   pub async fn commit(&mut self) -> Result<()> {
      self.execute_query(sqlx::query("COMMIT")).await?;
      Ok(())
   }

   /// Rollback the current transaction
   pub async fn rollback(&mut self) -> Result<()> {
      self.execute_query(sqlx::query("ROLLBACK")).await?;
      Ok(())
   }

   /// Detach all attached databases if this is an attached writer
   pub async fn detach_if_attached(self) -> Result<()> {
      if let Self::Attached(w) = self {
         w.detach_all().await?;
      }
      Ok(())
   }
}

impl From<WriterGuard> for TransactionWriter {
   fn from(guard: WriterGuard) -> Self {
      match guard {
         WriterGuard::Regular(w) => TransactionWriter::Regular(w),
         #[cfg(feature = "observer")]
         WriterGuard::Observable(w) => TransactionWriter::Observable(w),
      }
   }
}

/// Active transaction state holding the writer and metadata
#[must_use = "if unused, the transaction is immediately rolled back"]
pub struct ActiveInterruptibleTransaction {
   db_path: String,
   transaction_id: String,
   writer: Option<TransactionWriter>,
   created_at: Instant,
}

impl ActiveInterruptibleTransaction {
   pub fn new(db_path: String, transaction_id: String, writer: TransactionWriter) -> Self {
      Self {
         db_path,
         transaction_id,
         writer: Some(writer),
         created_at: Instant::now(),
      }
   }

   fn writer_mut(&mut self) -> Result<&mut TransactionWriter> {
      self
         .writer
         .as_mut()
         .ok_or(Error::TransactionAlreadyFinalized)
   }

   fn take_writer(&mut self) -> Result<TransactionWriter> {
      self.writer.take().ok_or(Error::TransactionAlreadyFinalized)
   }

   pub fn db_path(&self) -> &str {
      &self.db_path
   }

   pub fn transaction_id(&self) -> &str {
      &self.transaction_id
   }

   /// Execute a read query within this transaction and return decoded results
   pub async fn read(
      &mut self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<IndexMap<String, JsonValue>>> {
      let mut q = sqlx::query(&query);
      for value in values {
         q = crate::wrapper::bind_value(q, value);
      }

      let rows = self.writer_mut()?.fetch_all(q).await?;

      let mut results = Vec::new();
      for row in rows {
         let mut value = IndexMap::default();
         for (i, column) in row.columns().iter().enumerate() {
            let v = row.try_get_raw(i)?;
            let v = crate::decode::to_json(v)?;
            value.insert(column.name().to_string(), v);
         }
         results.push(value);
      }

      Ok(results)
   }

   /// Continue transaction with additional statements
   ///
   /// Accepts either `Statement` structs or tuples of `(&str, Vec<JsonValue>)`.
   pub async fn continue_with<S: Into<Statement>, I: IntoIterator<Item = S>>(
      &mut self,
      statements: I,
   ) -> Result<Vec<WriteQueryResult>> {
      let mut results = Vec::new();
      let writer = self.writer_mut()?;
      for statement in statements {
         let statement = statement.into();
         let mut q = sqlx::query(&statement.query);
         for value in statement.values {
            q = crate::wrapper::bind_value(q, value);
         }
         let exec_result = writer.execute_query(q).await?;
         results.push(WriteQueryResult {
            rows_affected: exec_result.rows_affected(),
            last_insert_id: exec_result.last_insert_rowid(),
         });
      }
      Ok(results)
   }

   /// Commit this transaction
   pub async fn commit(mut self) -> Result<()> {
      let mut writer = self.take_writer()?;
      writer.commit().await?;

      let db_path = self.db_path.clone();
      writer.detach_if_attached().await?;

      debug!("Transaction committed for db: {}", db_path);
      Ok(())
   }

   /// Rollback this transaction
   pub async fn rollback(mut self) -> Result<()> {
      let mut writer = self.take_writer()?;
      writer.rollback().await?;

      let db_path = self.db_path.clone();
      if let Err(detach_err) = writer.detach_if_attached().await {
         tracing::error!("detach_all failed after rollback: {}", detach_err);
      }

      debug!("Transaction rolled back for db: {}", db_path);
      Ok(())
   }
}

/// Statement in a transaction with query and bind values
#[derive(Debug, Deserialize)]
pub struct Statement {
   pub query: String,
   pub values: Vec<JsonValue>,
}

impl From<(&str, Vec<JsonValue>)> for Statement {
   fn from((query, values): (&str, Vec<JsonValue>)) -> Self {
      Self {
         query: query.to_string(),
         values,
      }
   }
}

impl From<(String, Vec<JsonValue>)> for Statement {
   fn from((query, values): (String, Vec<JsonValue>)) -> Self {
      Self { query, values }
   }
}

impl Drop for ActiveInterruptibleTransaction {
   fn drop(&mut self) {
      // If writer is still present, it means commit/rollback wasn't called.
      // SQLite will automatically ROLLBACK the transaction when the connection
      // is returned to the pool if no explicit COMMIT was issued.
      if self.writer.is_some() {
         debug!(
            "Dropping transaction for db: {}, tx_id: {} (will auto-rollback)",
            self.db_path, self.transaction_id
         );
      }
   }
}

/// Default transaction timeout (5 minutes).
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(300);

/// Global state tracking all active interruptible transactions.
///
/// Enforces one interruptible transaction per database path and applies a configurable
/// timeout. Expired transactions are cleaned up lazily on the next `insert()` or
/// `remove()` call — no background task is needed.
///
/// Uses `Mutex` rather than `RwLock` because all operations require write access,
/// and `Mutex<T>` only requires `T: Send` (not `T: Sync`) — avoiding an
/// `unsafe impl Sync` that would otherwise be needed due to non-`Sync` inner
/// types (`PoolConnection`, raw pointers in observer guards).
#[derive(Clone)]
pub struct ActiveInterruptibleTransactions {
   inner: Arc<Mutex<HashMap<String, ActiveInterruptibleTransaction>>>,
   timeout: Duration,
}

impl Default for ActiveInterruptibleTransactions {
   fn default() -> Self {
      Self::new(DEFAULT_TRANSACTION_TIMEOUT)
   }
}

impl ActiveInterruptibleTransactions {
   /// Create a new instance with the given transaction timeout.
   pub fn new(timeout: Duration) -> Self {
      Self {
         inner: Arc::new(Mutex::new(HashMap::new())),
         timeout,
      }
   }

   pub async fn insert(&self, db_path: String, tx: ActiveInterruptibleTransaction) -> Result<()> {
      use std::collections::hash_map::Entry;
      let mut txs = self.inner.lock().await;

      match txs.entry(db_path.clone()) {
         Entry::Vacant(e) => {
            e.insert(tx);
            Ok(())
         }
         Entry::Occupied(mut e) => {
            // If the existing transaction has expired, drop it (auto-rollback) and
            // replace with the new one.
            if e.get().created_at.elapsed() >= self.timeout {
               warn!(
                  "Evicting expired transaction for db: {} (age: {:?}, timeout: {:?})",
                  db_path,
                  e.get().created_at.elapsed(),
                  self.timeout,
               );
               // Drop the expired transaction (auto-rollback) before inserting the new one
               let _expired = e.insert(tx);
               Ok(())
            } else {
               Err(Error::TransactionAlreadyActive(db_path))
            }
         }
      }
   }

   pub async fn abort_all(&self) {
      let mut txs = self.inner.lock().await;
      debug!("Aborting {} active interruptible transaction(s)", txs.len());

      for db_path in txs.keys() {
         debug!(
            "Dropping interruptible transaction for database: {}",
            db_path
         );
      }

      // Clear all transactions to drop WriteGuards and release locks
      // Dropping triggers auto-rollback via Drop trait
      txs.clear();
   }

   /// Remove and return transaction for commit/rollback.
   ///
   /// Returns `Err(Error::TransactionTimedOut)` if the transaction has exceeded the
   /// configured timeout. The expired transaction is dropped (auto-rolled-back) in
   /// that case.
   pub async fn remove(
      &self,
      db_path: &str,
      token_id: &str,
   ) -> Result<ActiveInterruptibleTransaction> {
      let mut txs = self.inner.lock().await;

      // Validate token before removal
      let tx = txs
         .get(db_path)
         .ok_or_else(|| Error::NoActiveTransaction(db_path.to_string()))?;

      if tx.transaction_id() != token_id {
         return Err(Error::InvalidTransactionToken);
      }

      // Check if the transaction has expired
      if tx.created_at.elapsed() >= self.timeout {
         warn!(
            "Transaction timed out for db: {} (age: {:?}, timeout: {:?})",
            db_path,
            tx.created_at.elapsed(),
            self.timeout,
         );
         // Drop the expired transaction (auto-rollback via Drop)
         txs.remove(db_path);
         return Err(Error::TransactionTimedOut(db_path.to_string()));
      }

      // Safe unwrap: we just confirmed the key exists above
      Ok(txs.remove(db_path).unwrap())
   }
}

/// Tracking for regular (non-pausable) transactions that are in-flight.
///
/// Holds abort handles so transactions can be cancelled on app exit.
#[derive(Clone, Default)]
pub struct ActiveRegularTransactions(Arc<RwLock<HashMap<String, AbortHandle>>>);

impl ActiveRegularTransactions {
   pub async fn insert(&self, key: String, abort_handle: AbortHandle) {
      let mut txs = self.0.write().await;
      txs.insert(key, abort_handle);
   }

   pub async fn remove(&self, key: &str) {
      let mut txs = self.0.write().await;
      txs.remove(key);
   }

   pub async fn abort_all(&self) {
      let mut txs = self.0.write().await;
      debug!("Aborting {} active regular transaction(s)", txs.len());

      for (key, abort_handle) in txs.iter() {
         debug!("Aborting regular transaction: {}", key);
         abort_handle.abort();
      }

      txs.clear();
   }
}

/// Cleanup all transactions on app exit.
pub async fn cleanup_all_transactions(
   interruptible: &ActiveInterruptibleTransactions,
   regular: &ActiveRegularTransactions,
) {
   debug!("Cleaning up all active transactions");

   interruptible.abort_all().await;
   regular.abort_all().await;

   debug!("Transaction cleanup initiated");
}
