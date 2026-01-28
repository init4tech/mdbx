use std::{cell::RefCell, fmt::Debug, ptr, sync::Arc};

use crate::{
    Environment, MdbxResult,
    error::mdbx_result,
    tx::{
        PtrSync, TxPtrAccess,
        access::PtrUnsync,
        cache::{Cache, DbCache, SharedCache},
    },
};
use ffi::{MDBX_TXN_RDONLY, MDBX_TXN_READWRITE, MDBX_txn_flags_t};

mod private {
    pub trait Sealed {}
    impl Sealed for super::Ro {}
    impl Sealed for super::Rw {}
    impl Sealed for super::RwSync {}
    impl Sealed for super::RoSync {}
}

/// Marker type for read-only transactions.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Ro;

/// Marker type for read-write transactions.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Rw;

/// Marker type for synchronized read-only transactions.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RoSync;

/// Marker type for synchronized read-write transactions.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RwSync;

/// Marker trait for transaction kinds with associated inner type.
///
/// The `Inner` associated type determines how the transaction pointer is
/// stored:
/// - For [`RO`]: Either `RoInner` (Arc/Weak) with feature `read-tx-timeouts`,
///   or raw pointer without it
/// - For [`RW`]: Always raw pointer (direct ownership)
pub trait TransactionKind: WriterKind + SyncKind {
    /// Construct a new transaction of this kind from the given environment.
    ///
    /// This does NOT register RwSync transactions with the environment's
    /// transaction manager; that is the caller's responsibility.
    #[doc(hidden)]
    fn new_from_env(env: Environment) -> MdbxResult<Self::Access> {
        let mut txn: *mut ffi::MDBX_txn = ptr::null_mut();
        unsafe {
            mdbx_result(ffi::mdbx_txn_begin_ex(
                env.env_ptr(),
                ptr::null_mut(),
                Self::OPEN_FLAGS,
                &mut txn,
                ptr::null_mut(),
            ))?;
        }

        Ok(Self::Access::from_ptr_and_env(txn, env))
    }

    /// Create a new tracing span for this transaction kind.
    #[doc(hidden)]
    fn new_span(txn_id: usize) -> tracing::Span {
        tracing::debug_span!(
            target: "libmdbx",
            "mdbx_txn",
            kind = %if Self::IS_READ_ONLY { "ro" } else { "rw" },
            sync = %if Self::SYNC { "sync" } else { "unsync" },
            txn_id = txn_id,
        )
    }
}

impl<T> TransactionKind for T where T: WriterKind + SyncKind {}

pub trait SyncKind {
    const SYNC: bool = false;

    /// The inner storage type for the transaction pointer.
    type Access: TxPtrAccess;

    /// Cache type used for this transaction kind.
    type Cache: Cache;
}

impl SyncKind for RoSync {
    const SYNC: bool = true;
    type Access = Arc<PtrSync>;
    type Cache = SharedCache;
}

impl SyncKind for RwSync {
    const SYNC: bool = true;
    type Access = Arc<PtrSync>;
    type Cache = SharedCache;
}

impl SyncKind for Ro {
    type Access = PtrUnsync;
    type Cache = RefCell<DbCache>;
}

impl SyncKind for Rw {
    type Access = PtrUnsync;
    type Cache = RefCell<DbCache>;
}

/// Marker trait for writable transaction kinds.
///
/// Primarily used for writing bounds of the form
/// `K: TransactionKind + WriteMarker`.
pub trait WriteMarker: private::Sealed {}

impl WriteMarker for Rw {}
impl WriteMarker for RwSync {}

pub trait WriterKind: private::Sealed + core::fmt::Debug + 'static {
    const IS_READ_ONLY: bool = true;

    const OPEN_FLAGS: MDBX_txn_flags_t =
        { if Self::IS_READ_ONLY { MDBX_TXN_RDONLY } else { MDBX_TXN_READWRITE } };
}

impl WriterKind for Ro {}

impl WriterKind for Rw {
    const IS_READ_ONLY: bool = false;
}
impl WriterKind for RoSync {}
impl WriterKind for RwSync {
    const IS_READ_ONLY: bool = false;
}
