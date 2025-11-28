use tauri::{Runtime, plugin::TauriPlugin};

mod commands;
mod error;

pub use error::{Error, Result};

/// Initializes the plugin.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
   tauri::plugin::Builder::new("sqlite")
      // .invoke_handler(tauri::generate_handler![
      //    commands::load,
      //    commands::execute,
      //    commands::fetch_all,
      //    commands::fetch_one,
      //    commands::close,
      //    commands::close_all,
      //    commands::remove
      // ])
      .build()
}
