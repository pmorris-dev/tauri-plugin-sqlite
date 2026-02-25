use std::collections::HashSet;

/// Configuration for the SQLite observer.
///
/// Controls which tables are observed, the capacity of the broadcast channel
/// used to deliver change notifications to subscribers, and whether to capture
/// column values in change notifications.
#[derive(Debug, Clone)]
pub struct ObserverConfig {
   /// Tables to observe for changes.
   pub tables: HashSet<String>,

   /// Capacity of the broadcast channel for change notifications.
   ///
   /// **Important:** All changes in a transaction are delivered at once on commit.
   /// If your transaction contains more mutating SQL statements (INSERT/UPDATE/DELETE)
   /// than this capacity, **messages will be dropped**. Set this value to at least
   /// your largest expected transaction size.
   ///
   /// When messages are dropped, subscribers receive
   /// [`tokio::sync::broadcast::error::RecvError::Lagged`] on their next receive:
   ///
   /// ```no_run
   /// use tokio::sync::broadcast::error::RecvError;
   /// use sqlx_sqlite_observer::TableChange;
   ///
   /// async fn handle_changes(mut rx: tokio::sync::broadcast::Receiver<TableChange>) {
   ///     match rx.recv().await {
   ///         Ok(change) => { /* process normally */ }
   ///         Err(RecvError::Lagged(n)) => {
   ///             // Missed n changes - consider re-querying full state
   ///             // Better yet, fix the bug by increasing channel_capacity
   ///             tracing::warn!("Missed {} change notifications", n);
   ///         }
   ///         Err(RecvError::Closed) => { /* observer dropped */ }
   ///     }
   /// }
   /// ```
   ///
   /// Default: 256.
   ///
   /// [`TableChange`]: crate::TableChange
   pub channel_capacity: usize,

   /// Whether to capture column values in change notifications.
   ///
   /// When `true` (default), [`TableChange`] includes `old_values` and `new_values`
   /// with the actual column data before/after the change. When `false`, these
   /// fields are `None`, reducing memory usage per notification.
   ///
   /// **Note:** This affects memory per message, not overflow likelihood. Overflow
   /// is determined by the *count* of messages, not their size.
   ///
   /// Set to `false` if you only need to know *which* rows changed (table + rowid)
   /// and will re-query the data yourself.
   ///
   /// [`TableChange`]: crate::TableChange
   pub capture_values: bool,
}

impl Default for ObserverConfig {
   fn default() -> Self {
      Self {
         tables: HashSet::new(),
         channel_capacity: 256,
         capture_values: true,
      }
   }
}

impl ObserverConfig {
   /// Creates a new observer configuration with default settings.
   ///
   /// Defaults: no tables observed, channel capacity of 256, value capture enabled.
   pub fn new() -> Self {
      Self::default()
   }

   /// Sets the tables to observe for changes.
   ///
   /// Only changes to these tables will generate notifications.
   pub fn with_tables<I, S>(mut self, tables: I) -> Self
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      self.tables = tables.into_iter().map(Into::into).collect();
      self
   }

   /// Sets the broadcast channel capacity for change notifications.
   ///
   /// Capacity must be at least 1. A capacity of 0 will cause a panic when the
   /// observer is initialized.
   ///
   /// See [`channel_capacity`](Self::channel_capacity) for details on sizing.
   pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
      self.channel_capacity = capacity;
      self
   }

   /// Controls whether column values are captured in change notifications.
   ///
   /// When enabled (default), `TableChange` will include `old_values` and
   /// `new_values` with the actual column data. When disabled, these fields
   /// will be `None`, which is faster but provides less information.
   pub fn with_capture_values(mut self, capture: bool) -> Self {
      self.capture_values = capture;
      self
   }
}
