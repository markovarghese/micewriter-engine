use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
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
    /// If true, WriteBatch commits use `sync=true` so records hit disk before ACK.
    sync_writes: bool,
}

impl RocksStore {
    /// Open (or create) the RocksDB instance at `path`.
    pub fn open(path: &str, sync_writes: bool) -> Result<Self> {
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

        // Candidates are CFs that this engine could have created: the initial
        // bare "active" CF and any "active_<unix_ts>" rotated CFs. The newest
        // by parsed timestamp becomes the active target; everything else is an
        // orphan from a previous run that didn't finish flushing.
        //
        // We can't rely on `DB::list_cf`'s ordering or on lexicographic sort
        // (which breaks when timestamp digit-counts change), so we parse the
        // suffix explicitly. The initial bare "active" CF has no suffix and
        // is treated as the oldest possible.
        let mut candidates: Vec<(u64, String)> = cfs
            .iter()
            .filter(|n| n.as_str() != "default" && active_cf_timestamp(n).is_some())
            .map(|n| (active_cf_timestamp(n).unwrap(), n.clone()))
            .collect();
        candidates.sort_by_key(|(ts, _)| *ts);

        let mut active_name = INITIAL_CF.to_string();
        let mut orphans = Vec::new();
        if let Some((_, newest)) = candidates.last().cloned() {
            active_name = newest;
            for (_, name) in candidates.iter().take(candidates.len() - 1) {
                orphans.push(name.clone());
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
            sync_writes,
        })
    }

    /// Append a batch of serialised records to the active column family in a
    /// single RocksDB WriteBatch. Returns Ok once RocksDB confirms the write
    /// (and the OS fsync, if `sync_writes` is enabled).
    pub fn append_batch(&self, values: &[&[u8]]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let cf_name = self.active_cf.read().unwrap().clone();
        let db_lock = self.db.read().unwrap();
        let cf = db_lock
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow!("CF '{}' not found", cf_name))?;

        let mut batch = WriteBatch::default();
        for value in values {
            let key = self.counter.fetch_add(1, Ordering::Relaxed).to_be_bytes();
            batch.put_cf(&cf, key, value);
        }

        let mut wo = WriteOptions::default();
        wo.set_sync(self.sync_writes);
        db_lock.write_opt(batch, &wo)?;
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

/// Parse the timestamp embedded in an "active_<unix_ts>" CF name. Returns
/// `Some(0)` for the bare initial CF "active" so it sorts as the oldest, and
/// `None` for anything that isn't a CF this engine created (so callers can
/// filter it out).
fn active_cf_timestamp(name: &str) -> Option<u64> {
    if name == "active" {
        return Some(0);
    }
    let suffix = name.strip_prefix("active_")?;
    suffix.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::active_cf_timestamp;

    #[test]
    fn initial_cf_parses_as_oldest() {
        assert_eq!(active_cf_timestamp("active"), Some(0));
    }

    #[test]
    fn rotated_cf_parses_timestamp() {
        assert_eq!(active_cf_timestamp("active_1780124079"), Some(1780124079));
        assert_eq!(active_cf_timestamp("active_0"), Some(0));
    }

    #[test]
    fn non_engine_cfs_return_none() {
        assert_eq!(active_cf_timestamp("default"), None);
        assert_eq!(active_cf_timestamp("frozen_1780124079"), None);
        assert_eq!(active_cf_timestamp("active_notnumeric"), None);
        assert_eq!(active_cf_timestamp("activex"), None);
        assert_eq!(active_cf_timestamp(""), None);
    }

    #[test]
    fn newest_wins_after_digit_boundary() {
        // 9999999999 sorts AFTER 10000000000 lexicographically — make sure we
        // pick the numerically-newer one regardless.
        let mut cands: Vec<(u64, &str)> = ["active_9999999999", "active_10000000000"]
            .iter()
            .map(|n| (active_cf_timestamp(n).unwrap(), *n))
            .collect();
        cands.sort_by_key(|(ts, _)| *ts);
        assert_eq!(cands.last().unwrap().1, "active_10000000000");
    }
}
