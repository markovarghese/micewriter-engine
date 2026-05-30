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
    db: Arc<RwLock<DB>>,
    /// Name of the CF currently receiving new record appends.
    active_cf: Arc<RwLock<String>>,
    /// Monotonically increasing record key (8-byte big-endian).
    counter: AtomicU64,
    /// Leftover CFs from previous runs that failed to flush.
    orphaned_cfs: Arc<RwLock<Vec<String>>>,
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

        let mut active_name = INITIAL_CF.to_string();
        let mut orphans = Vec::new();
        let mut non_frozen = Vec::new();

        for n in &cfs {
            if !n.starts_with("frozen_") && n != "default" {
                non_frozen.push(n.clone());
            }
        }
        
        if let Some(last) = non_frozen.last() {
            active_name = last.clone();
            for n in non_frozen.iter().take(non_frozen.len() - 1) {
                orphans.push(n.clone());
            }
        }

        // Determine max key in the active CF to avoid overwriting un-flushed records.
        let mut max_id: u64 = 0;
        {
            if let Some(cf) = db.cf_handle(&active_name) {
                let mut iter = db.iterator_cf(&cf, rocksdb::IteratorMode::End);
                if let Some(Ok((k, _))) = iter.next() {
                    if k.len() == 8 {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&k);
                        max_id = u64::from_be_bytes(buf);
                    }
                }
            }
        }

        info!(cf = %active_name, orphans = orphans.len(), max_key = max_id, "RocksDB opened, active column family");

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            active_cf: Arc::new(RwLock::new(active_name)),
            counter: AtomicU64::new(max_id + 1),
            orphaned_cfs: Arc::new(RwLock::new(orphans)),
        })
    }

    /// Append a serialised record to the active column family.
    pub fn append(&self, value: &[u8]) -> Result<()> {
        let key = self.counter.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let cf_name = self.active_cf.read().unwrap().clone();
        
        let db_lock = self.db.read().unwrap();
        let cf = db_lock
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow!("CF '{}' not found", cf_name))?;
        db_lock.put_cf(&cf, key, value)?;
        Ok(())
    }

    /// Rotate the active CF:
    ///  1. Creates a fresh CF that becomes the new active.
    ///  2. Returns the name of the frozen CF and all its records as raw bytes.
    ///
    /// The caller is responsible for flushing the records and then calling
    /// `drop_frozen_cf` once the Iceberg commit succeeds.
    pub fn rotate(&self) -> Result<String> {
        let frozen_name = {
            let mut active = self.active_cf.write().unwrap();
            let frozen = active.clone();

            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let new_cf = format!("active_{}", ts);

            self.db.write().unwrap().create_cf(&new_cf, &Options::default())?;
            *active = new_cf;
            frozen
        };

        info!(frozen = %frozen_name, "Column family rotated");

        Ok(frozen_name)
    }

    /// Retrieve and clear the list of orphaned column families.
    pub fn get_orphaned_cfs(&self) -> Vec<String> {
        let mut orphans = self.orphaned_cfs.write().unwrap();
        let result = orphans.clone();
        orphans.clear();
        result
    }

    /// Iterate over all records in a given column family without buffering them all in memory.
    pub fn iterate_cf<F>(&self, name: &str, mut f: F) -> Result<()> 
    where
        F: FnMut(&[u8]) -> Result<()>
    {
        let db_lock = self.db.read().unwrap();
        let cf = db_lock
            .cf_handle(name)
            .ok_or_else(|| anyhow!("CF '{}' not found", name))?;
            
        let iter = db_lock.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_, v) = item?;
            f(&v)?;
        }
        Ok(())
    }

    /// Drop the frozen column family after a successful Iceberg commit.
    pub fn drop_frozen_cf(&self, name: &str) -> Result<()> {
        self.db.write().unwrap().drop_cf(name)?;
        info!(cf = %name, "Frozen CF dropped");
        Ok(())
    }
}
