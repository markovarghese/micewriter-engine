use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use anyhow::{anyhow, Result};
use rand::Rng;
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
    /// Approximate uncompressed byte size of records in the active CF
    active_cf_bytes: AtomicU64,
    /// Current randomized flush size threshold
    active_cf_size_limit: AtomicU64,
    /// Base config values
    flush_size_bytes: u64,
    flush_size_jitter_bytes: u64,
    /// Frozen CFs awaiting a successful flush — both leftovers from a previous
    /// run and runtime-retained CFs whose Iceberg commit failed. Drained by
    /// `get_orphaned_cfs` at the start of each flush cycle and re-populated via
    /// `retain_frozen_cf` whenever a commit fails, so failed cycles are retried
    /// without needing a pod restart.
    orphaned_cfs: Arc<RwLock<Vec<String>>>,
    /// If true, WriteBatch commits use `sync=true` so records hit disk before ACK.
    sync_writes: bool,
    /// Global exact count of uncompiled bytes across all CFs
    total_unflushed_bytes: AtomicU64,
    /// Exact sizes of each frozen CF
    frozen_cf_sizes: Arc<RwLock<HashMap<String, u64>>>,
}

impl RocksStore {
    /// Open (or create) the RocksDB instance at `path`.
    pub fn new(
        path: impl AsRef<std::path::Path>,
        flush_size_bytes: u64,
        flush_size_jitter_bytes: u64,
        sync_writes: bool,
        write_buffer_size: usize,
    ) -> Result<Self> {
        let path = path.as_ref();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_bytes_per_sync(1048576);
        db_opts.set_use_direct_reads(true);
        db_opts.set_use_direct_io_for_flush_and_compaction(true);

        // List existing CFs so we re-open them all; RocksDB requires it.
        let cfs = match DB::list_cf(&db_opts, path) {
            Ok(names) => names,
            Err(_) => vec![INITIAL_CF.to_string()],
        };

        let mut cf_opts = Options::default();
        cf_opts.set_write_buffer_size(write_buffer_size);
        cf_opts.set_max_write_buffer_number(2);
        cf_opts.set_compression_type(rocksdb::DBCompressionType::None);
        cf_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::None);

        let cf_descriptors: Vec<_> = cfs
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, cf_opts.clone()))
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

        // Set initial total_unflushed_bytes by estimating orphans at flush_size_bytes
        let total_unflushed = orphans.len() as u64 * flush_size_bytes;
        let mut frozen_sizes = HashMap::new();
        for orphan in &orphans {
            frozen_sizes.insert(orphan.clone(), flush_size_bytes);
        }

        let store = Self {
            db: Arc::new(RwLock::new(db)),
            active_cf: Arc::new(RwLock::new(active_name)),
            counter: AtomicU64::new(max_id + 1),
            active_cf_bytes: AtomicU64::new(0),
            active_cf_size_limit: AtomicU64::new(0),
            flush_size_bytes,
            flush_size_jitter_bytes,
            orphaned_cfs: Arc::new(RwLock::new(orphans)),
            sync_writes,
            total_unflushed_bytes: AtomicU64::new(total_unflushed),
            frozen_cf_sizes: Arc::new(RwLock::new(frozen_sizes)),
        };
        store.reset_size_limit();
        Ok(store)
    }

    /// Append a batch of serialised records to the active column family in a
    /// single RocksDB WriteBatch. Returns Ok(true) if the byte limit was exceeded.
    pub fn append_batch(&self, values: &[&[u8]]) -> Result<bool> {
        if values.is_empty() {
            return Ok(false);
        }
        let cf_name = self.active_cf.read().unwrap().clone();
        let db_lock = self.db.read().unwrap();
        let cf = db_lock
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow!("CF '{}' not found", cf_name))?;

        let mut batch = WriteBatch::default();
        let mut batch_bytes: u64 = 0;
        for value in values {
            batch_bytes += value.len() as u64;
            let key = self.counter.fetch_add(1, Ordering::Relaxed).to_be_bytes();
            batch.put_cf(&cf, key, value);
        }

        let mut wo = WriteOptions::default();
        wo.set_sync(self.sync_writes);
        db_lock.write_opt(batch, &wo)?;
        
        let new_size = self.active_cf_bytes.fetch_add(batch_bytes, Ordering::Relaxed) + batch_bytes;
        self.total_unflushed_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
        
        let limit = self.active_cf_size_limit.load(Ordering::Relaxed);

        Ok(new_size >= limit)
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
                .as_micros();
            let new_cf = format!("active_{}", ts);

            let mut cf_opts = Options::default();
            cf_opts.set_write_buffer_size(4 * 1024 * 1024);
            cf_opts.set_max_write_buffer_number(2);
            cf_opts.set_compression_type(rocksdb::DBCompressionType::None);
            cf_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::None);
            self.db.write().unwrap().create_cf(&new_cf, &cf_opts)?;
            *active = new_cf;
            
            let frozen_bytes = self.active_cf_bytes.swap(0, Ordering::Relaxed);
            self.frozen_cf_sizes.write().unwrap().insert(frozen.clone(), frozen_bytes);
            self.reset_size_limit();
            
            frozen
        };

        info!(frozen = %frozen_name, "Column family rotated");

        Ok(frozen_name)
    }

    fn reset_size_limit(&self) {
        let jitter = rand::thread_rng().gen_range(0..=(self.flush_size_jitter_bytes * 2));
        let new_limit = self.flush_size_bytes
            .saturating_add(jitter)
            .saturating_sub(self.flush_size_jitter_bytes);
        self.active_cf_size_limit.store(new_limit.max(1024 * 1024), Ordering::Relaxed);
    }

    /// Retrieve and clear the list of orphaned column families.
    pub fn get_orphaned_cfs(&self) -> Vec<String> {
        let mut orphans = self.orphaned_cfs.write().unwrap();
        let result = orphans.clone();
        orphans.clear();
        result
    }

    /// Re-add a frozen CF to the retention list after a failed flush so it is
    /// retried on the next cycle. The CF itself stays in RocksDB; only the
    /// in-memory tracking is restored.
    pub fn retain_frozen_cf(&self, name: String) {
        self.orphaned_cfs.write().unwrap().push(name);
    }

    /// Snapshot count of currently retained frozen CFs. Hot-path safe — reads
    /// take a brief RwLock read guard. Used by the ingest handler to apply
    /// backpressure when flushes are persistently failing.
    pub fn retained_cf_count(&self) -> usize {
        self.orphaned_cfs.read().unwrap().len()
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
        
        if let Some(size) = self.frozen_cf_sizes.write().unwrap().remove(name) {
            self.total_unflushed_bytes.fetch_sub(size, Ordering::Relaxed);
        }
        
        info!(cf = %name, "Frozen CF dropped");
        Ok(())
    }

    /// Global total of unflushed bytes across all active and frozen CFs.
    pub fn total_unflushed_bytes(&self) -> u64 {
        self.total_unflushed_bytes.load(Ordering::Relaxed)
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
