use crate::{
    Cursor, Database, Environment, MdbxError, ReadResult, Stat, TableObject,
    error::{MdbxResult, mdbx_result},
    flags::{DatabaseFlags, WriteFlags},
    sys::txn_manager::{TxnManagerMessage, TxnPtr},
};
use ffi::{MDBX_TXN_RDONLY, MDBX_TXN_READWRITE, MDBX_txn_flags_t};
use parking_lot::{Mutex, MutexGuard, RwLock};
use smallvec::SmallVec;
use std::{
    ffi::{CStr, c_uint, c_void},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    mem::size_of,
    ptr, slice,
    sync::{Arc, atomic::AtomicBool, mpsc::sync_channel},
    time::Duration,
};

mod private {
    use super::*;

    pub trait Sealed {}

    impl Sealed for RO {}
    impl Sealed for RW {}
}

/// Marker trait for transaction kinds. Either [`RO`] or [`RW`].
pub trait TransactionKind: private::Sealed + Send + Sync + Debug + 'static {
    #[doc(hidden)]
    const OPEN_FLAGS: MDBX_txn_flags_t;

    /// Convenience flag for distinguishing between read-only and read-write transactions.
    #[doc(hidden)]
    const IS_READ_ONLY: bool;
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
/// Marker type for read-only transactions.
pub struct RO;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
/// Marker type for read-write transactions.
pub struct RW;

impl TransactionKind for RO {
    const OPEN_FLAGS: MDBX_txn_flags_t = MDBX_TXN_RDONLY;
    const IS_READ_ONLY: bool = true;
}
impl TransactionKind for RW {
    const OPEN_FLAGS: MDBX_txn_flags_t = MDBX_TXN_READWRITE;
    const IS_READ_ONLY: bool = false;
}

/// Cached database entry.
///
/// Uses hash-only comparison since 64-bit hash collisions are negligible
/// for practical database counts.
#[derive(Debug, Clone, Copy)]
struct CachedDb {
    /// Hash of database name (None hashes distinctly from any string).
    name_hash: u64,
    /// The cached database (dbi + flags).
    db: Database,
}

impl From<CachedDb> for Database {
    fn from(value: CachedDb) -> Self {
        value.db
    }
}

/// Simple cache container for database handles.
///
/// Uses inline storage for the common case (most apps use < 16 databases).
#[derive(Debug)]
struct DbCache {
    entries: RwLock<SmallVec<[CachedDb; 16]>>,
}

impl DbCache {
    /// Creates a new empty cache.
    fn new() -> Self {
        Self { entries: RwLock::new(SmallVec::new()) }
    }

    /// Returns a read guard to the cache entries.
    fn read(&self) -> parking_lot::RwLockReadGuard<'_, SmallVec<[CachedDb; 16]>> {
        self.entries.read()
    }

    /// Returns a write guard to the cache entries.
    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, SmallVec<[CachedDb; 16]>> {
        self.entries.write()
    }

    /// Read a database entry from the cache.
    fn read_db(&self, name_hash: u64) -> Option<Database> {
        let entries = self.read();
        for entry in entries.iter() {
            if entry.name_hash == name_hash {
                return Some(entry.db);
            }
        }
        None
    }

    /// Write a database entry to the cache.
    fn write_db(&self, db: CachedDb) {
        let mut entries = self.write();
        for entry in entries.iter() {
            if entry.name_hash == db.name_hash {
                return; // Another thread beat us
            }
        }
        entries.push(db);
    }

    /// Remove a database entry from the cache by dbi.
    fn remove_dbi(&self, dbi: ffi::MDBX_dbi) {
        let mut entries = self.write();
        entries.retain(|entry| entry.db.dbi() != dbi);
    }
}

impl Default for DbCache {
    fn default() -> Self {
        Self::new()
    }
}

/// An MDBX transaction.
///
/// All database operations require a transaction.
pub struct Transaction<K>
where
    K: TransactionKind,
{
    inner: Arc<TransactionInner<K>>,
}

impl<K> Transaction<K>
where
    K: TransactionKind,
{
    pub(crate) fn new(env: Environment) -> MdbxResult<Self> {
        let mut txn: *mut ffi::MDBX_txn = ptr::null_mut();
        unsafe {
            mdbx_result(ffi::mdbx_txn_begin_ex(
                env.env_ptr(),
                ptr::null_mut(),
                K::OPEN_FLAGS,
                &mut txn,
                ptr::null_mut(),
            ))?;
            Ok(Self::new_from_ptr(env, txn))
        }
    }

    pub(crate) fn new_from_ptr(env: Environment, txn_ptr: *mut ffi::MDBX_txn) -> Self {
        let txn = TransactionPtr::new(txn_ptr);

        #[cfg(feature = "read-tx-timeouts")]
        if K::IS_READ_ONLY {
            env.txn_manager().add_active_read_transaction(txn_ptr, txn.clone())
        }

        let inner = TransactionInner {
            txn,
            committed: AtomicBool::new(false),
            env,
            db_cache: DbCache::default(),
            _marker: Default::default(),
        };

        Self { inner: Arc::new(inner) }
    }

    /// Executes the given closure once the lock on the transaction is acquired.
    ///
    /// The caller **must** ensure that the pointer is not used after the
    /// lifetime of the transaction.
    #[inline]
    pub fn txn_execute<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        self.inner.txn_execute(f)
    }

    /// Executes the given closure once the lock on the transaction is acquired. If the transaction
    /// is timed out, it will be renewed first.
    ///
    /// Returns the result of the closure or an error if the transaction renewal fails.
    #[inline]
    pub(crate) fn txn_execute_renew_on_timeout<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        self.inner.txn_execute_renew_on_timeout(f)
    }

    /// Returns a copy of the raw pointer to the underlying MDBX transaction.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub(crate) fn txn(&self) -> *mut ffi::MDBX_txn {
        self.inner.txn.txn
    }

    /// Returns a raw pointer to the MDBX environment.
    pub fn env(&self) -> &Environment {
        &self.inner.env
    }

    /// Returns the transaction id.
    pub fn id(&self) -> MdbxResult<u64> {
        self.txn_execute(|txn| unsafe { ffi::mdbx_txn_id(txn) })
    }

    /// Gets an item from a database.
    ///
    /// This function retrieves the data associated with the given key in the
    /// database. If the database supports duplicate keys
    /// ([`DatabaseFlags::DUP_SORT`]) then the first data item for the key will be
    /// returned. Retrieval of other items requires the use of
    /// [Cursor]. If the item is not in the database, then
    /// [None] will be returned.
    pub fn get<'a, Key>(&'a self, dbi: ffi::MDBX_dbi, key: &[u8]) -> ReadResult<Option<Key>>
    where
        Key: TableObject<'a>,
    {
        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        self.txn_execute(|txn| unsafe {
            match ffi::mdbx_get(txn, dbi, &key_val, &mut data_val) {
                ffi::MDBX_SUCCESS => Key::decode_val::<K>(self, data_val).map(Some),
                ffi::MDBX_NOTFOUND => Ok(None),
                err_code => Err(MdbxError::from_err_code(err_code).into()),
            }
        })?
    }

    /// Commits the transaction.
    ///
    /// Any pending operations will be saved.
    pub fn commit(self) -> MdbxResult<CommitLatency> {
        match self.txn_execute(|txn| {
            if K::IS_READ_ONLY {
                #[cfg(feature = "read-tx-timeouts")]
                self.env().txn_manager().remove_active_read_transaction(txn);

                let mut latency = CommitLatency::new();
                mdbx_result(unsafe { ffi::mdbx_txn_commit_ex(txn, latency.mdb_commit_latency()) })
                    .map(|v| (v, latency))
            } else {
                let (sender, rx) = sync_channel(0);
                self.env()
                    .txn_manager()
                    .send_message(TxnManagerMessage::Commit { tx: TxnPtr(txn), sender });
                rx.recv().unwrap()
            }
        })? {
            //
            Ok((false, lat)) => {
                self.inner.set_committed();
                Ok(lat)
            }
            Ok((true, _)) => {
                // MDBX_RESULT_TRUE means the transaction was aborted due to prior errors.
                // The transaction is still finished/freed by MDBX, so we must mark it as
                // committed to prevent the Drop impl from trying to abort it again.
                self.inner.set_committed();
                Err(MdbxError::BotchedTransaction)
            }
            Err(e) => Err(e),
        }
    }

    /// Opens a handle to an MDBX database, and cache the handle for re-use.
    ///
    /// If `name` is `None`, then the returned handle will be for the default
    /// database.
    ///
    /// If `name` is not `None`, then the returned handle will be for a named
    /// database. In this case the environment must be configured to allow
    /// named databases through
    /// [`EnvironmentBuilder::set_max_dbs()`](crate::EnvironmentBuilder::set_max_dbs).
    ///
    /// The returned database handle MAY be shared among any transaction in the
    /// environment. However, if the tx is RW and the DB is created within the
    /// tx, the DB will not be visible to other transactions until the tx is
    /// committed.
    ///
    /// The database name MAY NOT contain the null character.
    pub fn open_db(&self, name: Option<&str>) -> MdbxResult<Database> {
        let name_hash = Self::hash_name(name);

        if let Some(db) = self.inner.db_cache.read_db(name_hash) {
            return Ok(db);
        }

        self.open_and_cache_with_flags(name, DatabaseFlags::empty()).map(Into::into)
    }

    /// Open a DB handle without checking or writing to the cache.
    ///
    /// This may be useful when the transaction intends to open many (>20)
    /// tables, as cache performance will degrade slightly with size.
    pub fn open_db_no_cache(&self, name: Option<&str>) -> MdbxResult<Database> {
        self.open_db_with_flags(name, DatabaseFlags::empty()).map(Into::into)
    }

    /// Raw open (don't check cache) with flags. Write to cache after opening.
    fn open_and_cache_with_flags(
        &self,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> Result<CachedDb, MdbxError> {
        // Slow path: open via FFI and cache
        let db = self.open_db_with_flags(name, flags)?;

        // Double-check pattern to avoid duplicate entries
        self.inner.db_cache.write_db(db);

        Ok(db)
    }

    /// Raw open (don't check cache) with flags.
    ///
    /// Return the name hash along with the database.
    fn open_db_with_flags(&self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<CachedDb> {
        let mut c_name_buf = SmallVec::<[u8; 32]>::new();
        let c_name = name.map(|n| {
            c_name_buf.extend_from_slice(n.as_bytes());
            c_name_buf.push(0);
            CStr::from_bytes_with_nul(&c_name_buf).unwrap()
        });
        let name_ptr = c_name.as_ref().map_or(ptr::null(), |s| s.as_ptr());

        // Single txn_execute: open dbi AND read flags
        let db = self.txn_execute(|txn_ptr| {
            let mut dbi: ffi::MDBX_dbi = 0;
            mdbx_result(unsafe { ffi::mdbx_dbi_open(txn_ptr, name_ptr, flags.bits(), &mut dbi) })?;

            // Read actual flags (may differ from requested due to ACCEDE)
            let mut actual_flags: c_uint = 0;
            let mut _status: c_uint = 0;
            mdbx_result(unsafe {
                ffi::mdbx_dbi_flags_ex(txn_ptr, dbi, &mut actual_flags, &mut _status)
            })?;

            // The types are not the same on Windows. Great!
            #[cfg_attr(not(windows), allow(clippy::useless_conversion))]
            let db_flags = DatabaseFlags::from_bits_truncate(actual_flags.try_into().unwrap());

            Ok(Database::new(dbi, db_flags))
        })??;
        Ok(CachedDb { name_hash: Self::hash_name(name), db })
    }

    #[inline]
    fn hash_name(name: Option<&str>) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish()
    }

    /// Gets the option flags for the given database in the transaction.
    pub fn db_flags(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<DatabaseFlags> {
        let mut flags: c_uint = 0;
        unsafe {
            self.txn_execute(|txn| {
                // `mdbx_dbi_flags_ex` requires `status` to be a non-NULL ptr, otherwise it will
                // return an EINVAL and panic below, so we just provide a placeholder variable
                // which we discard immediately.
                let mut _status: c_uint = 0;
                mdbx_result(ffi::mdbx_dbi_flags_ex(txn, dbi, &mut flags, &mut _status))
            })??;
        }

        // The types are not the same on Windows. Great!
        #[cfg_attr(not(windows), allow(clippy::useless_conversion))]
        Ok(DatabaseFlags::from_bits_truncate(flags.try_into().unwrap()))
    }

    /// Retrieves database statistics.
    pub fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        self.db_stat_with_dbi(dbi)
    }

    /// Retrieves database statistics by the given dbi.
    pub fn db_stat_with_dbi(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        unsafe {
            let mut stat = Stat::new();
            self.txn_execute(|txn| {
                mdbx_result(ffi::mdbx_dbi_stat(txn, dbi, stat.mdb_stat(), size_of::<Stat>()))
            })??;
            Ok(stat)
        }
    }

    /// Open a new cursor on the given database.
    pub fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, K>> {
        Cursor::new(self, db)
    }

    /// Open a new cursor on the given dbi.
    #[deprecated(since = "0.2.0", note = "use `cursor(&Database)` instead")]
    pub fn cursor_with_dbi(&self, db: Database) -> MdbxResult<Cursor<'_, K>> {
        Cursor::new(self, db)
    }

    /// Disables a timeout for this read transaction.
    #[cfg(feature = "read-tx-timeouts")]
    pub fn disable_timeout(&self) {
        if K::IS_READ_ONLY {
            self.env().txn_manager().remove_active_read_transaction(self.inner.txn.txn);
        }
    }
}

impl<K> Clone for Transaction<K>
where
    K: TransactionKind,
{
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<K> fmt::Debug for Transaction<K>
where
    K: TransactionKind,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoTransaction").finish_non_exhaustive()
    }
}

/// Internals of a transaction.
struct TransactionInner<K>
where
    K: TransactionKind,
{
    /// The transaction pointer itself.
    txn: TransactionPtr,
    /// Whether the transaction has committed.
    committed: AtomicBool,

    /// Hold open the environment.
    env: Environment,

    /// Cache of opened database handles.
    db_cache: DbCache,

    _marker: std::marker::PhantomData<fn() -> K>,
}

impl<K> TransactionInner<K>
where
    K: TransactionKind,
{
    /// Marks the transaction as committed.
    fn set_committed(&self) {
        self.committed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn has_committed(&self) -> bool {
        self.committed.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[inline]
    fn txn_execute<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        self.txn.txn_execute_fail_on_timeout(f)
    }

    #[inline]
    fn txn_execute_renew_on_timeout<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        self.txn.txn_execute_renew_on_timeout(f)
    }
}

impl<K> Drop for TransactionInner<K>
where
    K: TransactionKind,
{
    fn drop(&mut self) {
        // To be able to abort a timed out transaction, we need to renew it
        // first. Hence the usage of `txn_execute_renew_on_timeout` here.
        self.txn
            .txn_execute_renew_on_timeout(|txn| {
                if !self.has_committed() {
                    if K::IS_READ_ONLY {
                        #[cfg(feature = "read-tx-timeouts")]
                        self.env.txn_manager().remove_active_read_transaction(txn);

                        unsafe {
                            ffi::mdbx_txn_abort(txn);
                        }
                    } else {
                        let (sender, rx) = sync_channel(0);
                        self.env
                            .txn_manager()
                            .send_message(TxnManagerMessage::Abort { tx: TxnPtr(txn), sender });
                        rx.recv().unwrap().unwrap();
                    }
                }
            })
            .unwrap();
    }
}

impl Transaction<RW> {
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
        self.open_db_with_flags(name, flags | DatabaseFlags::CREATE).map(|db| db.db)
    }

    /// Stores an item into a database.
    ///
    /// This function stores key/data pairs in the database. The default
    /// behavior is to enter the new key/data pair, replacing any previously
    /// existing key if duplicates are disallowed, or adding a duplicate data
    /// item if duplicates are allowed ([`DatabaseFlags::DUP_SORT`]).
    pub fn put(
        &self,
        dbi: ffi::MDBX_dbi,
        key: impl AsRef<[u8]>,
        data: impl AsRef<[u8]>,
        flags: WriteFlags,
    ) -> MdbxResult<()> {
        let key = key.as_ref();
        let data = data.as_ref();
        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: data.len(), iov_base: data.as_ptr() as *mut c_void };
        mdbx_result(self.txn_execute(|txn| unsafe {
            ffi::mdbx_put(txn, dbi, &key_val, &mut data_val, flags.bits())
        })?)?;

        Ok(())
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
        dbi: ffi::MDBX_dbi,
        key: impl AsRef<[u8]>,
        len: usize,
        flags: WriteFlags,
    ) -> MdbxResult<&mut [u8]> {
        let key = key.as_ref();
        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: len, iov_base: ptr::null_mut::<c_void>() };
        unsafe {
            mdbx_result(self.txn_execute(|txn| {
                ffi::mdbx_put(txn, dbi, &key_val, &mut data_val, flags.bits() | ffi::MDBX_RESERVE)
            })?)?;
            Ok(slice::from_raw_parts_mut(data_val.iov_base as *mut u8, data_val.iov_len))
        }
    }

    /// Reserves space for a value of the given length at the given key, and
    /// calls the given closure with a mutable slice to write into.
    ///
    /// This is a safe wrapper around [`Transaction::reserve`].
    pub fn with_reservation(
        &self,
        dbi: ffi::MDBX_dbi,
        key: impl AsRef<[u8]>,
        len: usize,
        flags: WriteFlags,
        f: impl FnOnce(&mut [u8]),
    ) -> MdbxResult<()> {
        let buf = unsafe { self.reserve(dbi, key, len, flags)? };
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
        dbi: ffi::MDBX_dbi,
        key: impl AsRef<[u8]>,
        data: Option<&[u8]>,
    ) -> MdbxResult<bool> {
        let key = key.as_ref();
        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let data_val: Option<ffi::MDBX_val> = data.map(|data| ffi::MDBX_val {
            iov_len: data.len(),
            iov_base: data.as_ptr() as *mut c_void,
        });

        mdbx_result({
            let ptr = data_val.as_ref().map_or(ptr::null(), |d| d as *const ffi::MDBX_val);
            self.txn_execute(|txn| unsafe { ffi::mdbx_del(txn, dbi, &key_val, ptr) })?
        })
        .map(|_| true)
        .or_else(|e| match e {
            MdbxError::NotFound => Ok(false),
            other => Err(other),
        })
    }

    /// Empties the given database. All items will be removed.
    pub fn clear_db(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        mdbx_result(self.txn_execute(|txn| unsafe { ffi::mdbx_drop(txn, dbi, false) })?)?;

        Ok(())
    }

    /// Drops the database from the environment.
    ///
    /// # Safety
    /// Caller must close ALL other [Database] and [Cursor] instances pointing
    /// to the same dbi BEFORE calling this function.
    pub unsafe fn drop_db(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        mdbx_result(self.txn_execute(|txn| unsafe { ffi::mdbx_drop(txn, dbi, true) })?)?;

        self.inner.db_cache.remove_dbi(dbi);

        Ok(())
    }
}

impl Transaction<RO> {
    /// Closes the database handle.
    ///
    /// # Safety
    /// Caller must close ALL other [Database] and [Cursor] instances pointing to the same dbi
    /// BEFORE calling this function.
    pub unsafe fn close_db(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        mdbx_result(unsafe { ffi::mdbx_dbi_close(self.env().env_ptr(), dbi) })?;

        self.inner.db_cache.remove_dbi(dbi);

        Ok(())
    }
}

impl Transaction<RW> {
    /// Begins a new nested transaction inside of this transaction.
    pub fn begin_nested_txn(&mut self) -> MdbxResult<Self> {
        if self.inner.env.is_write_map() {
            return Err(MdbxError::NestedTransactionsUnsupportedWithWriteMap);
        }
        self.txn_execute(|txn| {
            let (tx, rx) = sync_channel(0);
            self.env().txn_manager().send_message(TxnManagerMessage::Begin {
                parent: TxnPtr(txn),
                flags: RW::OPEN_FLAGS,
                sender: tx,
            });

            rx.recv().unwrap().map(|ptr| Self::new_from_ptr(self.env().clone(), ptr.0))
        })?
    }
}

/// A shareable pointer to an MDBX transaction.
#[derive(Debug, Clone)]
pub(crate) struct TransactionPtr {
    txn: *mut ffi::MDBX_txn,
    #[cfg(feature = "read-tx-timeouts")]
    timed_out: Arc<AtomicBool>,
    lock: Arc<Mutex<()>>,
}

impl TransactionPtr {
    fn new(txn: *mut ffi::MDBX_txn) -> Self {
        Self {
            txn,
            #[cfg(feature = "read-tx-timeouts")]
            timed_out: Arc::new(AtomicBool::new(false)),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Returns `true` if the transaction is timed out.
    ///
    /// When transaction is timed out via `TxnManager`, it's actually reset using
    /// `mdbx_txn_reset`. It makes the transaction unusable (MDBX fails on any usages of such
    /// transactions).
    ///
    /// Importantly, we can't rely on `MDBX_TXN_FINISHED` flag to check if the transaction is timed
    /// out using `mdbx_txn_reset`, because MDBX uses it in other cases too.
    #[cfg(feature = "read-tx-timeouts")]
    fn is_timed_out(&self) -> bool {
        self.timed_out.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(feature = "read-tx-timeouts")]
    pub(crate) fn set_timed_out(&self) {
        self.timed_out.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Acquires the inner transaction lock to guarantee exclusive access to the transaction
    /// pointer.
    fn lock(&self) -> MutexGuard<'_, ()> {
        if let Some(lock) = self.lock.try_lock() {
            lock
        } else {
            tracing::trace!(
                target: "libmdbx",
                txn = %self.txn as usize,
                backtrace = %std::backtrace::Backtrace::capture(),
                "Transaction lock is already acquired, blocking...
                To display the full backtrace, run with `RUST_BACKTRACE=full` env variable."
            );
            self.lock.lock()
        }
    }

    /// Executes the given closure once the lock on the transaction is acquired.
    ///
    /// Returns the result of the closure or an error if the transaction is
    /// timed out.
    #[inline]
    pub(crate) fn txn_execute_fail_on_timeout<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        let _lck = self.lock();

        // No race condition with the `TxnManager` timing out the transaction
        // is possible here, because we're taking a lock for any actions on the
        // transaction pointer, including a call to the `mdbx_txn_reset`.
        #[cfg(feature = "read-tx-timeouts")]
        if self.is_timed_out() {
            return Err(MdbxError::ReadTransactionTimeout);
        }

        Ok((f)(self.txn))
    }

    /// Executes the given closure once the lock on the transaction is
    /// acquired. If the transaction is timed out, it will be renewed first.
    ///
    /// Returns the result of the closure or an error if the transaction renewal fails.
    #[inline]
    pub(crate) fn txn_execute_renew_on_timeout<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        let _lck = self.lock();

        // To be able to do any operations on the transaction, we need to renew it first.
        #[cfg(feature = "read-tx-timeouts")]
        if self.is_timed_out() {
            mdbx_result(unsafe { ffi::mdbx_txn_renew(self.txn) })?;
        }

        Ok((f)(self.txn))
    }
}

/// Commit latencies info.
///
/// Contains information about latency of commit stages.
/// Inner struct stores this info in 1/65536 of seconds units.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct CommitLatency(ffi::MDBX_commit_latency);

impl CommitLatency {
    /// Create a new `CommitLatency` with zero'd inner struct `ffi::MDBX_commit_latency`.
    pub(crate) const fn new() -> Self {
        unsafe { Self(std::mem::zeroed()) }
    }

    /// Returns a mut pointer to `ffi::MDBX_commit_latency`.
    pub(crate) const fn mdb_commit_latency(&mut self) -> *mut ffi::MDBX_commit_latency {
        &mut self.0
    }
}

impl CommitLatency {
    /// Duration of preparation (commit child transactions, update
    /// sub-databases records and cursors destroying).
    #[inline]
    pub const fn preparation(&self) -> Duration {
        Self::time_to_duration(self.0.preparation)
    }

    /// Duration of GC update by wall clock.
    #[inline]
    pub const fn gc_wallclock(&self) -> Duration {
        Self::time_to_duration(self.0.gc_wallclock)
    }

    /// Duration of internal audit if enabled.
    #[inline]
    pub const fn audit(&self) -> Duration {
        Self::time_to_duration(self.0.audit)
    }

    /// Duration of writing dirty/modified data pages to a filesystem,
    /// i.e. the summary duration of a `write()` syscalls during commit.
    #[inline]
    pub const fn write(&self) -> Duration {
        Self::time_to_duration(self.0.write)
    }

    /// Duration of syncing written data to the disk/storage, i.e.
    /// the duration of a `fdatasync()` or a `msync()` syscall during commit.
    #[inline]
    pub const fn sync(&self) -> Duration {
        Self::time_to_duration(self.0.sync)
    }

    /// Duration of transaction ending (releasing resources).
    #[inline]
    pub const fn ending(&self) -> Duration {
        Self::time_to_duration(self.0.ending)
    }

    /// The total duration of a commit.
    #[inline]
    pub const fn whole(&self) -> Duration {
        Self::time_to_duration(self.0.whole)
    }

    /// User-mode CPU time spent on GC update.
    #[inline]
    pub const fn gc_cputime(&self) -> Duration {
        Self::time_to_duration(self.0.gc_cputime)
    }

    #[inline]
    const fn time_to_duration(time: u32) -> Duration {
        Duration::from_nanos(time as u64 * (1_000_000_000 / 65_536))
    }
}

// SAFETY: Access to the transaction is synchronized by the lock.
unsafe impl Send for TransactionPtr {}

// SAFETY: Access to the transaction is synchronized by the lock.
unsafe impl Sync for TransactionPtr {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const fn assert_send_sync<T: Send + Sync>() {}

    #[expect(dead_code)]
    const fn test_txn_send_sync() {
        assert_send_sync::<Transaction<RO>>();
        assert_send_sync::<Transaction<RW>>();
    }

    #[test]
    fn test_db_cache_returns_same_db() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_ro_txn().unwrap();

        let db1 = txn.open_db(None).unwrap();
        let db2 = txn.open_db(None).unwrap();

        assert_eq!(db1.dbi(), db2.dbi());
        assert_eq!(db1.flags(), db2.flags());
    }

    #[test]
    fn test_db_cache_no_cache_still_works() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_ro_txn().unwrap();

        let db1 = txn.open_db_no_cache(None).unwrap();
        let db2 = txn.open_db_no_cache(None).unwrap();

        // Same DBI should be returned by MDBX
        assert_eq!(db1.dbi(), db2.dbi());
    }

    #[test]
    fn test_db_cache_cached_matches_uncached() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_ro_txn().unwrap();

        let cached = txn.open_db(None).unwrap();
        let uncached = txn.open_db_no_cache(None).unwrap();

        assert_eq!(cached.dbi(), uncached.dbi());
        assert_eq!(cached.flags(), uncached.flags());
    }

    #[test]
    fn test_db_cache_multiple_named_dbs() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().set_max_dbs(10).open(dir.path()).unwrap();

        // Create named DBs
        {
            let txn = env.begin_rw_txn().unwrap();
            txn.create_db(Some("db1"), DatabaseFlags::empty()).unwrap();
            txn.create_db(Some("db2"), DatabaseFlags::empty()).unwrap();
            txn.commit().unwrap();
        }

        let txn = env.begin_ro_txn().unwrap();

        let db1_a = txn.open_db(Some("db1")).unwrap();
        let db2_a = txn.open_db(Some("db2")).unwrap();
        let db1_b = txn.open_db(Some("db1")).unwrap();
        let db2_b = txn.open_db(Some("db2")).unwrap();

        // Same named DB returns same handle
        assert_eq!(db1_a.dbi(), db1_b.dbi());
        assert_eq!(db2_a.dbi(), db2_b.dbi());

        // Different DBs have different handles
        assert_ne!(db1_a.dbi(), db2_a.dbi());
    }

    #[test]
    fn test_db_cache_flags_preserved() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().set_max_dbs(10).open(dir.path()).unwrap();

        // Create DB with specific flags
        {
            let txn = env.begin_rw_txn().unwrap();
            txn.create_db(Some("dupsort"), DatabaseFlags::DUP_SORT).unwrap();
            txn.commit().unwrap();
        }

        let txn = env.begin_ro_txn().unwrap();
        let db = txn.open_db(Some("dupsort")).unwrap();

        assert!(db.flags().contains(DatabaseFlags::DUP_SORT));

        // Second open should have same flags from cache
        let db2 = txn.open_db(Some("dupsort")).unwrap();
        assert!(db2.flags().contains(DatabaseFlags::DUP_SORT));
    }
}
