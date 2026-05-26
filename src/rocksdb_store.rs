use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tracing::info;

const INITIAL_CF: &str = "active";

/// Thread-safe RocksDB wrapper that manages the active/frozen column-family
/// rotation used to implement the jittered flush cycle.
pub struct RocksStore {
    db: Arc<DB>,
    /// Name of the CF currently receiving new record appends.
    active_cf: Arc<RwLock<String>>,
    /// Monotonically increasing record key (8-byte big-endian).
    counter: AtomicU64,
}

impl RocksStore {
    /// Open (or create) the RocksDB instance at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // List existing CFs so we re-open them all; RocksDB requires it.
        let cfs = match DB::list_cf(&db_opts, path) {
            Ok(names) => names,
            Err(_) => vec![INITIAL_CF.to_string()],
        };

        let cf_descriptors: Vec<_> = cfs
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)?;

        // Determine which CF should be active (the most recently created one
        // whose name doesn't start with "frozen_").
        let active_name = cfs
            .iter()
            .filter(|n| !n.starts_with("frozen_"))
            .last()
            .cloned()
            .unwrap_or_else(|| INITIAL_CF.to_string());

        info!(cf = %active_name, "RocksDB opened, active column family");

        Ok(Self {
            db: Arc::new(db),
            active_cf: Arc::new(RwLock::new(active_name)),
            counter: AtomicU64::new(0),
        })
    }

    /// Append a serialised record to the active column family.
    pub fn append(&self, value: &[u8]) -> Result<()> {
        let key = self.counter.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let cf_name = self.active_cf.read().unwrap().clone();
        let cf = self
            .db
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow!("CF '{}' not found", cf_name))?;
        self.db.put_cf(&cf, key, value)?;
        Ok(())
    }

    /// Rotate the active CF:
    ///  1. Creates a fresh CF that becomes the new active.
    ///  2. Returns the name of the frozen CF and all its records as raw bytes.
    ///
    /// The caller is responsible for flushing the records and then calling
    /// `drop_frozen_cf` once the Iceberg commit succeeds.
    pub fn rotate(&self) -> Result<(String, Vec<Vec<u8>>)> {
        let frozen_name = {
            let mut active = self.active_cf.write().unwrap();
            let frozen = active.clone();

            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let new_cf = format!("active_{}", ts);

            self.db.create_cf(&new_cf, &Options::default())?;
            *active = new_cf;
            frozen
        };

        info!(frozen = %frozen_name, "Column family rotated");

        // Drain all records from the now-frozen CF.
        let cf = self
            .db
            .cf_handle(&frozen_name)
            .ok_or_else(|| anyhow!("frozen CF '{}' not found", frozen_name))?;

        let records = self
            .db
            .full_iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .map(|r| r.map(|(_, v)| v.to_vec()))
            .collect::<Result<Vec<_>, _>>()?;

        info!(count = records.len(), frozen = %frozen_name, "Records drained from frozen CF");

        Ok((frozen_name, records))
    }

    /// Drop the frozen column family after a successful Iceberg commit.
    pub fn drop_frozen_cf(&self, name: &str) -> Result<()> {
        self.db.drop_cf(name)?;
        info!(cf = %name, "Frozen CF dropped");
        Ok(())
    }
}
