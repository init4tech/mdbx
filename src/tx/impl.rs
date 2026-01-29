use crate::{
    CommitLatency, Cursor, Database, DatabaseFlags, Environment, MdbxError, MdbxResult, ReadResult,
    Ro, Rw, Stat, TableObject, TransactionKind, WriteFlags,
    error::mdbx_result,
    sys::txn_manager::{Begin, Commit, CommitLatencyPtr, RawTxPtr},
    tx::aliases::{RoTxSync, RoTxUnsync, RwTxUnsync},
    tx::{
        PtrSync, PtrUnsync, TxPtrAccess,
        cache::{Cache, CachedDb},
        kind::{RoSync, SyncKind, WriteMarker, WriterKind},
        ops,
    },
};
use core::fmt;
use ffi::MDBX_commit_latency;
use smallvec::SmallVec;
use std::{
    ffi::CStr,
    ptr,
    sync::{Arc, mpsc::sync_channel},
    thread::sleep,
    time::Duration,
};
use tracing::{debug_span, instrument, warn};

#[cfg(debug_assertions)]
use crate::tx::assertions;

/// Meta-data for a transaction.
#[derive(Clone)]
struct TxMeta {
    env: Environment,
    span: tracing::Span,
}

impl fmt::Debug for TxMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxMeta").finish()
    }
}

/// An MDBX transaction.
///
/// Prefer using the [`TxSync`] or
/// [`TxUnsync`] type aliases, unless specifically
/// implementing generic code over all four transaction kinds.
///
/// [`TxSync`]: crate::tx::aliases::TxSync
/// [`TxUnsync`]: crate::tx::aliases::TxUnsync
pub struct Tx<K: TransactionKind, U = <K as SyncKind>::Access> {
    txn: U,

    cache: K::Cache,

    meta: TxMeta,
}

impl<K: TransactionKind, U> fmt::Debug for Tx<K, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx").finish_non_exhaustive()
    }
}

impl<K> Clone for Tx<K, Arc<PtrSync>>
where
    K: TransactionKind<Access = Arc<PtrSync>>,
{
    fn clone(&self) -> Self {
        Self { txn: Arc::clone(&self.txn), cache: self.cache.clone(), meta: self.meta.clone() }
    }
}

impl<K: TransactionKind> Tx<K> {
    /// Creates a new transaction wrapper.
    pub(crate) fn from_access_and_env(txn: K::Access, env: Environment) -> Self {
        let span = K::new_span(txn.tx_id().unwrap_or_default());
        let meta = TxMeta { env, span };
        let cache = K::Cache::default();
        Self { txn, cache, meta }
    }

    /// Creates a new transaction wrapper from raw pointer and environment.
    pub(crate) fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment) -> Self {
        let tx = K::Access::from_ptr_and_env(ptr, env.clone(), K::IS_READ_ONLY);
        Self::from_access_and_env(tx, env)
    }

    /// Returns a reference to the environment.
    #[inline(always)]
    pub const fn env(&self) -> &Environment {
        &self.meta.env
    }

    /// Returns the tracing span for this transaction.
    #[inline(always)]
    pub const fn span(&self) -> &tracing::Span {
        &self.meta.span
    }
}

impl RoTxSync {
    pub(crate) fn begin(env: Environment) -> Result<Self, MdbxError> {
        let tx = RoSync::new_from_env(env.clone())?;
        Ok(Self::from_access_and_env(tx, env))
    }
}

impl RwTxUnsync {
    pub(crate) fn begin(env: Environment) -> Result<Self, MdbxError> {
        let tx = Rw::new_from_env(env.clone())?;
        Ok(Self::from_access_and_env(tx, env))
    }
}

impl RoTxUnsync {
    pub(crate) fn begin(env: Environment) -> Result<Self, MdbxError> {
        let tx = Ro::new_from_env(env.clone())?;
        Ok(Self::from_access_and_env(tx, env))
    }
}

// Unified implementations for all transaction kinds.
impl<K> Tx<K>
where
    K: TransactionKind,
{
    /// Provides access to the raw transaction pointer.
    fn with_txn_ptr<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        self.txn.with_txn_ptr(f)
    }

    /// Returns the transaction id.
    #[inline(always)]
    pub fn id(&self) -> MdbxResult<u64> {
        self.with_txn_ptr(|txn_ptr| Ok(unsafe { ffi::mdbx_txn_id(txn_ptr) }))
    }

    /// Gets an item from a database.
    pub fn get<'a, Key>(&'a self, dbi: ffi::MDBX_dbi, key: &[u8]) -> ReadResult<Option<Key>>
    where
        Key: TableObject<'a>,
    {
        self.with_txn_ptr(|txn_ptr| {
            // SAFETY: txn_ptr is valid from with_txn_ptr.
            unsafe {
                let data_val = ops::get_raw(txn_ptr, dbi, key)?;
                data_val.map(|val| Key::decode_val::<K>(txn_ptr, val)).transpose()
            }
        })
    }

    /// Opens a handle to an MDBX database.
    pub fn open_db(&self, name: Option<&str>) -> MdbxResult<Database> {
        let name_hash = CachedDb::hash_name(name);

        if let Some(db) = self.cache.read_db(name_hash) {
            return Ok(db);
        }

        self.open_and_cache_with_flags(name, DatabaseFlags::empty()).map(Into::into)
    }

    /// Opens a database handle without using the cache.
    pub fn open_db_no_cache(&self, name: Option<&str>) -> MdbxResult<Database> {
        self.open_db_with_flags(name, DatabaseFlags::empty()).map(Into::into)
    }

    fn open_and_cache_with_flags(
        &self,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> MdbxResult<CachedDb> {
        let db = self.open_db_with_flags(name, flags)?;
        self.cache.write_db(db);
        Ok(db)
    }

    fn open_db_with_flags(&self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<CachedDb> {
        let mut c_name_buf = SmallVec::<[u8; 32]>::new();
        let c_name = name.map(|n| {
            c_name_buf.extend_from_slice(n.as_bytes());
            c_name_buf.push(0);
            CStr::from_bytes_with_nul(&c_name_buf).unwrap()
        });
        let name_ptr = c_name.as_ref().map_or(ptr::null(), |s| s.as_ptr());

        let (dbi, db_flags) = self.with_txn_ptr(|txn_ptr| {
            // SAFETY: txn_ptr is valid from with_txn_ptr, name_ptr is valid or null.
            unsafe { ops::open_db_raw(txn_ptr, name_ptr, flags) }
        })?;

        Ok(CachedDb::new(name, Database::new(dbi, db_flags)))
    }

    /// Gets the option flags for the given database.
    pub fn db_flags(&self, name: Option<&str>) -> MdbxResult<DatabaseFlags> {
        let db = self.open_db(name)?;
        self.db_flags_by_dbi(db.dbi())
    }

    /// Gets the option flags for the given database.
    pub fn db_flags_by_dbi(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<DatabaseFlags> {
        self.with_txn_ptr(|txn_ptr| {
            // SAFETY: txn_ptr is valid from with_txn_ptr.
            unsafe { ops::db_flags_raw(txn_ptr, dbi) }
        })
    }

    /// Retrieves database statistics.
    pub fn db_stat(&self, db: &Database) -> MdbxResult<Stat> {
        self.db_stat_by_dbi(db.dbi())
    }

    /// Retrieves database statistics by the given dbi.
    pub fn db_stat_by_dbi(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        self.with_txn_ptr(|txn| {
            // SAFETY: txn is a valid transaction pointer from with_txn_ptr.
            unsafe { ops::db_stat_raw(txn, dbi) }
        })
    }

    /// Closes the database handle.
    ///
    /// # Safety
    ///
    /// This will invalidate data cached in [`Database`] instances with the
    /// DBI, and may result in bad behavior when using those instances after
    /// calling this function.
    pub unsafe fn close_db(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        // SAFETY: Caller ensures no other references exist.
        unsafe { ops::close_db_raw(self.meta.env.env_ptr(), dbi) }?;
        self.cache.remove_dbi(dbi);
        Ok(())
    }

    /// Opens a cursor on the given database.
    ///
    /// Multiple cursors can be open simultaneously on different databases
    /// within the same transaction. The cursor borrows the transaction's
    /// inner access type, allowing concurrent cursor operations.
    pub fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, K>> {
        Cursor::new(&self.txn, db)
    }
}

// Write-only
impl<K: TransactionKind + WriteMarker> Tx<K> {
    /// Opens a handle to an MDBX database, creating the database if necessary.
    ///
    /// If the database is already created, the given option flags will be
    /// added to it.
    ///
    /// If `name` is [None], then the returned handle will be for the default
    /// database.
    ///
    /// If `name` is not [None], then the returned handle will be for a named
    /// database. In this case the environment must be configured to allow
    /// named databases through [`EnvironmentBuilder::set_max_dbs()`].
    ///
    /// This function will fail with [`MdbxError::BadRslot`] if called by a
    /// thread with an open transaction.
    ///
    /// [`EnvironmentBuilder::set_max_dbs()`]: crate::EnvironmentBuilder::set_max_dbs
    pub fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database> {
        self.open_db_with_flags(name, flags | DatabaseFlags::CREATE).map(Into::into)
    }

    /// Stores an item into a database.
    ///
    /// This function stores key/data pairs in the database. The default
    /// behavior is to enter the new key/data pair, replacing any previously
    /// existing key if duplicates are disallowed, or adding a duplicate data
    /// item if duplicates are allowed ([`DatabaseFlags::DUP_SORT`]).
    pub fn put(
        &self,
        db: Database,
        key: impl AsRef<[u8]>,
        data: impl AsRef<[u8]>,
        flags: WriteFlags,
    ) -> MdbxResult<()> {
        let key = key.as_ref();
        let data = data.as_ref();

        #[cfg(debug_assertions)]
        {
            use crate::tx::assertions;

            let pagesize = self.env().stat().map(|s| s.page_size() as usize).unwrap_or(4096);
            assertions::debug_assert_put(pagesize, db.flags(), key, data);
        }

        self.with_txn_ptr(|txn| {
            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe { ops::put_raw(txn, db.dbi(), key, data, flags) }
        })
    }

    /// Appends a key/data pair to the end of the database.
    ///
    /// The key must be greater than all existing keys (or less than, for
    /// [`DatabaseFlags::REVERSE_KEY`] tables). This is more efficient than
    /// [`Tx::put`] when adding data in sorted order.
    ///
    /// In debug builds, this method asserts that the key ordering constraint is
    /// satisfied.
    pub fn append(
        &self,
        db: Database,
        key: impl AsRef<[u8]>,
        data: impl AsRef<[u8]>,
    ) -> MdbxResult<()> {
        let key = key.as_ref();
        let data = data.as_ref();

        self.with_txn_ptr(|txn| {
            #[cfg(debug_assertions)]
            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe {
                ops::debug_assert_append(txn, db.dbi(), db.flags(), key, data);
            }

            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe { ops::put_raw(txn, db.dbi(), key, data, WriteFlags::APPEND) }
        })
    }

    /// Appends duplicate data for [`DatabaseFlags::DUP_SORT`] databases.
    ///
    /// The data must be greater than all existing data for this key (or less
    /// than, for [`DatabaseFlags::REVERSE_DUP`] tables). This is more efficient
    /// than [`Tx::put`] when adding duplicates in sorted order.
    ///
    /// In debug builds, this method asserts that the data ordering constraint
    /// is satisfied.
    pub fn append_dup(
        &self,
        db: Database,
        key: impl AsRef<[u8]>,
        data: impl AsRef<[u8]>,
    ) -> MdbxResult<()> {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(db.flags());

        let key = key.as_ref();
        let data = data.as_ref();

        self.with_txn_ptr(|txn| {
            #[cfg(debug_assertions)]
            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe {
                ops::debug_assert_append_dup(txn, db.dbi(), db.flags(), key, data);
            }

            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe { ops::put_raw(txn, db.dbi(), key, data, WriteFlags::APPEND_DUP) }
        })
    }

    /// Returns a buffer which can be used to write a value into the item at the
    /// given key and with the given length. The buffer must be completely
    /// filled by the caller.
    ///
    /// This should not be used on dupsort tables.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the returned buffer is not used after the
    /// transaction is committed or aborted, or if another value is inserted.
    /// To be clear: the second call to this function is not permitted while
    /// the returned slice is reachable.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn reserve(
        &self,
        db: Database,
        key: impl AsRef<[u8]>,
        len: usize,
        flags: WriteFlags,
    ) -> MdbxResult<&mut [u8]> {
        let key = key.as_ref();

        #[cfg(debug_assertions)]
        {
            use crate::tx::assertions;

            let pagesize = self.env().stat().map(|s| s.page_size() as usize).unwrap_or(4096);
            assertions::debug_assert_key(pagesize, db.flags(), key);
        }

        let ptr = self.with_txn_ptr(|txn| {
            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe { ops::reserve_raw(txn, db.dbi(), key, len, flags) }
        })?;
        // SAFETY: ptr is valid from reserve_raw, len matches.
        Ok(unsafe { ops::slice_from_reserved(ptr, len) })
    }

    /// Reserves space for a value of the given length at the given key, and
    /// calls the given closure with a mutable slice to write into.
    ///
    /// This is a safe wrapper around [`Tx::reserve`].
    pub fn with_reservation(
        &self,
        db: Database,
        key: impl AsRef<[u8]>,
        len: usize,
        flags: WriteFlags,
        f: impl FnOnce(&mut [u8]),
    ) -> MdbxResult<()> {
        let buf = unsafe { self.reserve(db, key, len, flags)? };
        f(buf);
        Ok(())
    }

    /// Delete items from a database.
    /// This function removes key/data pairs from the database.
    ///
    /// The data parameter is NOT ignored regardless the database does support
    /// sorted duplicate data items or not. If the data parameter is [Some]
    /// only the matching data item will be deleted. Otherwise, if data
    /// parameter is [None], any/all value(s) for specified key will
    /// be deleted.
    ///
    /// Returns `true` if the key/value pair was present.
    pub fn del(
        &self,
        db: Database,
        key: impl AsRef<[u8]>,
        data: Option<&[u8]>,
    ) -> MdbxResult<bool> {
        let key = key.as_ref();

        #[cfg(debug_assertions)]
        {
            use crate::tx::assertions;

            let pagesize = self.env().stat().map(|s| s.page_size() as usize).unwrap_or(4096);
            assertions::debug_assert_key(pagesize, db.flags(), key);
            if let Some(v) = data {
                assertions::debug_assert_value(pagesize, db.flags(), v);
            }
        }

        self.with_txn_ptr(|txn| {
            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe { ops::del_raw(txn, db.dbi(), key, data) }
        })
    }

    /// Empties the given database. All items will be removed.
    pub fn clear_db(&self, db: Database) -> MdbxResult<()> {
        self.with_txn_ptr(|txn| {
            // SAFETY: txn is a valid RW transaction pointer from with_txn_ptr.
            unsafe { ops::clear_db_raw(txn, db.dbi()) }
        })
    }

    /// Drops the database from the environment.
    ///
    /// # Safety
    ///
    /// Caller must ensure no [`Cursor`] or other references to the database
    /// exist. [`Database`] instances with the DBI will be invalidated, and
    /// use after calling this function may result in bad behavior.
    pub unsafe fn drop_db(&self, db: Database) -> MdbxResult<()> {
        self.with_txn_ptr(|txn| {
            // SAFETY: txn is a valid RW transaction pointer, caller ensures
            // no other references to dbi exist.
            unsafe { ops::drop_db_raw(txn, db.dbi()) }
        })?;

        self.cache.remove_dbi(db.dbi());

        Ok(())
    }
}

// Differentiated Commit implementations for Sync and Unsync transaction
// pointers.
impl<K> Tx<K, Arc<PtrSync>>
where
    K: TransactionKind<Access = Arc<PtrSync>>,
{
    /// Commits the transaction.
    ///
    /// Any pending operations will be saved.
    ///
    /// SAFETY: latency pointer must be valid for the duration of the commit.
    fn commit_inner(self, latency: *mut MDBX_commit_latency) -> MdbxResult<()> {
        let was_aborted = self.with_txn_ptr(|txn| {
            if K::IS_READ_ONLY {
                mdbx_result(unsafe { ffi::mdbx_txn_commit_ex(txn, latency) })
            } else {
                let (sender, rx) = sync_channel(0);
                self.env().txn_manager().send(Commit {
                    tx: RawTxPtr(txn),
                    latency: CommitLatencyPtr(latency),
                    span: debug_span!("tx_manager_commit"),
                    sender,
                });
                rx.recv().unwrap()
            }
        })?;

        self.txn.mark_committed();

        if was_aborted {
            tracing::warn!(target: "libmdbx", "botched");
            return Err(MdbxError::BotchedTransaction);
        }

        Ok(())
    }

    /// Commits the transaction.
    #[instrument(skip(self), parent = &self.meta.span)]
    pub fn commit(self) -> MdbxResult<()> {
        self.commit_inner(ptr::null_mut())
    }

    /// Commits the transaction, returning commit latency information.
    #[instrument(skip(self), parent = &self.meta.span)]
    pub fn commit_with_latency(self) -> MdbxResult<CommitLatency> {
        let mut latency = CommitLatency::new();

        self.commit_inner(latency.mdb_commit_latency())?;

        tracing::debug!(latency_whole_ms = latency.whole().as_millis(), "commit latency");
        Ok(latency)
    }
}

impl<K> Tx<K, PtrUnsync>
where
    K: TransactionKind<Access = PtrUnsync>,
{
    /// Commits the transaction (inner implementation).
    fn commit_inner(self, latency: *mut ffi::MDBX_commit_latency) -> MdbxResult<()> {
        // Self is dropped at end of function, so RwTxPtr::drop will be within
        // span scope.
        let _guard = self.meta.span.clone().entered();

        // SAFETY: txn_ptr is valid from with_txn_ptr.
        let was_aborted =
            self.with_txn_ptr(|txn_ptr| unsafe { ops::commit_raw(txn_ptr, latency) })?;

        self.txn.mark_committed();

        if was_aborted {
            tracing::warn!(target: "libmdbx", "botched");
            return Err(MdbxError::BotchedTransaction);
        }

        Ok(())
    }

    /// Commits the transaction.
    #[instrument(skip(self), parent = &self.meta.span)]
    pub fn commit(self) -> MdbxResult<()> {
        self.commit_inner(ptr::null_mut())
    }

    /// Commits the transaction, returning commit latency information.
    #[instrument(skip(self), parent = &self.meta.span)]
    pub fn commit_with_latency(self) -> MdbxResult<CommitLatency> {
        let mut latency = CommitLatency::new();

        self.commit_inner(latency.mdb_commit_latency())?;

        tracing::debug!(latency_whole_ms = latency.whole().as_millis(), "commit latency");
        Ok(latency)
    }
}

// Differentiated nested transaction implementations for Sync and Unsync
// transaction pointers.
impl<K> Tx<K, Arc<PtrSync>>
where
    K: TransactionKind<Access = Arc<PtrSync>> + WriteMarker,
{
    /// Begins a new [`RwTxSync`](crate::tx::aliases::RwTxSync) transaction.
    pub fn begin(env: Environment) -> MdbxResult<Self> {
        let mut warned = false;
        let txn = loop {
            let (tx, rx) = sync_channel(0);
            env.txn_manager().send(Begin {
                parent: RawTxPtr(ptr::null_mut()),
                flags: Rw::OPEN_FLAGS,
                sender: tx,
                span: debug_span!("txn_manager_begin"),
            });
            let res = rx.recv().unwrap();
            if matches!(&res, Err(MdbxError::Busy)) {
                if !warned {
                    warned = true;
                    warn!(target: "libmdbx", "Process stalled, awaiting read-write transaction lock.");
                }
                sleep(Duration::from_millis(250));
                continue;
            }

            break res;
        }?;

        Ok(Self::from_ptr_and_env(txn.0, env))
    }

    /// Begins a new nested transaction inside of this transaction.
    pub fn begin_nested_txn(&self) -> MdbxResult<Self> {
        if self.env().is_write_map() {
            return Err(MdbxError::NestedTransactionsUnsupportedWithWriteMap);
        }
        self.with_txn_ptr(|txn| {
            let (tx, rx) = sync_channel(0);
            self.env().txn_manager().send(Begin {
                parent: RawTxPtr(txn),
                flags: Rw::OPEN_FLAGS,
                sender: tx,
                span: debug_span!("tx_manager_begin_nested"),
            });

            rx.recv().unwrap().map(|txn| Self::from_ptr_and_env(txn.0, self.env().clone()))
        })
    }
}

impl<K> Tx<K, PtrUnsync>
where
    K: TransactionKind<Access = PtrUnsync> + WriteMarker,
{
    /// Begins a new nested transaction inside of this transaction.
    pub fn begin_nested_txn(&mut self) -> MdbxResult<Self> {
        if self.env().is_write_map() {
            return Err(MdbxError::NestedTransactionsUnsupportedWithWriteMap);
        }
        self.with_txn_ptr(|txn_ptr| {
            // SAFETY: txn_ptr is valid from with_txn_ptr.
            unsafe {
                let mut nested_txn: *mut ffi::MDBX_txn = ptr::null_mut();
                mdbx_result(ffi::mdbx_txn_begin_ex(
                    self.env().env_ptr(),
                    txn_ptr,
                    Rw::OPEN_FLAGS,
                    &mut nested_txn,
                    ptr::null_mut(),
                ))?;
                Ok(Self::from_ptr_and_env(nested_txn, self.env().clone()))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::aliases::{RoTxSync, RwTxSync, TxUnsync};
    use tempfile::tempdir;

    #[test]
    fn test_basic_rw_operations() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Write data
        let txn = TxUnsync::<Rw>::begin(env.clone()).unwrap();
        let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
        txn.put(db, b"key1", b"value1", WriteFlags::empty()).unwrap();
        txn.put(db, b"key2", b"value2", WriteFlags::empty()).unwrap();
        txn.commit().unwrap();

        // Read data
        let txn = TxUnsync::<Ro>::begin(env.clone()).unwrap();

        let db = txn.open_db(None).unwrap();
        let value: Option<Vec<u8>> = txn.get(db.dbi(), b"key1").unwrap();
        assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

        let value: Option<Vec<u8>> = txn.get(db.dbi(), b"key2").unwrap();
        assert_eq!(value.as_deref(), Some(b"value2".as_slice()));

        let value: Option<Vec<u8>> = txn.get(db.dbi(), b"nonexistent").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_db_cache() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().set_max_dbs(10).open(dir.path()).unwrap();

        // Create named DBs
        {
            let txn = TxUnsync::<Rw>::begin(env.clone()).unwrap();
            txn.create_db(Some("db1"), DatabaseFlags::empty()).unwrap();
            txn.create_db(Some("db2"), DatabaseFlags::empty()).unwrap();
            txn.commit().unwrap();
        }

        let txn = TxUnsync::<Ro>::begin(env.clone()).unwrap();

        let db1_a = txn.open_db(Some("db1")).unwrap();
        let db1_b = txn.open_db(Some("db1")).unwrap();
        let db2 = txn.open_db(Some("db2")).unwrap();

        assert_eq!(db1_a.dbi(), db1_b.dbi());
        assert_ne!(db1_a.dbi(), db2.dbi());
    }

    fn __compile_checks() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<RoTxSync>();
        assert_sync::<RwTxSync>();
        assert_sync::<TxMeta>();

        fn assert_send<T: Send>() {}
        assert_send::<RoTxSync>();
        assert_send::<RwTxSync>();
        assert_send::<RoTxUnsync>();
        assert_send::<TxMeta>();
    }
}
