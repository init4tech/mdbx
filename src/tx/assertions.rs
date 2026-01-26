//! Debug assertions to catch MDBX constraint violations before they hit C assertions.
//!
//! These assertions validate key and value size constraints at the Rust level,
//! preventing MDBX's internal `cASSERT` from aborting when `MDBX_FORCE_ASSERTIONS`
//! or `MDBX_DEBUG` is enabled.
//!
//! These functions are only used in debug builds, so they are allowed to be dead
//! code in release builds.

#![allow(dead_code)]

use crate::flags::DatabaseFlags;

/// Debug assertion that validates key size constraints.
///
/// MDBX has a maximum key size that depends on the page size and database flags.
/// This assertion catches oversized keys in debug builds before they reach MDBX.
#[inline]
#[allow(clippy::missing_const_for_fn)] // Cannot be const when debug_assertions calls FFI
pub(crate) fn debug_assert_key_size(pagesize: usize, flags: DatabaseFlags, key: &[u8]) {
    #[cfg(debug_assertions)]
    {
        let max_key =
            unsafe { ffi::mdbx_limits_keysize_max(pagesize as isize, flags.bits()) } as isize;
        debug_assert!(
            max_key >= 0 && key.len() <= max_key as usize,
            "Key size {} exceeds maximum {} for this database (pagesize={}, flags={:?})",
            key.len(),
            max_key,
            pagesize,
            flags
        );
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (pagesize, flags, key);
    }
}

/// Debug assertion that validates value size constraints.
///
/// MDBX has a maximum value size that depends on the page size and database flags.
/// This assertion catches oversized values in debug builds before they reach MDBX.
#[inline]
#[allow(clippy::missing_const_for_fn)] // Cannot be const when debug_assertions calls FFI
pub(crate) fn debug_assert_value_size(pagesize: usize, flags: DatabaseFlags, value: &[u8]) {
    #[cfg(debug_assertions)]
    {
        let max_val =
            unsafe { ffi::mdbx_limits_valsize_max(pagesize as isize, flags.bits()) } as isize;
        debug_assert!(
            max_val >= 0 && value.len() <= max_val as usize,
            "Value size {} exceeds maximum {} for this database (pagesize={}, flags={:?})",
            value.len(),
            max_val,
            pagesize,
            flags
        );
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (pagesize, flags, value);
    }
}

/// Debug assertion that validates key size for INTEGER_KEY databases (must be 4 or 8 bytes).
#[inline]
pub(crate) fn debug_assert_integer_key(flags: DatabaseFlags, key: &[u8]) {
    debug_assert!(
        !flags.contains(DatabaseFlags::INTEGER_KEY) || key.len() == 4 || key.len() == 8,
        "INTEGER_KEY database requires key length of 4 or 8 bytes, got {}",
        key.len()
    );
}

/// Debug assertion that validates value size for INTEGER_DUP databases (must be 4 or 8 bytes).
#[inline]
pub(crate) fn debug_assert_integer_dup(flags: DatabaseFlags, value: &[u8]) {
    debug_assert!(
        !flags.contains(DatabaseFlags::INTEGER_DUP) || value.len() == 4 || value.len() == 8,
        "INTEGER_DUP database requires value length of 4 or 8 bytes, got {}",
        value.len()
    );
}

/// Runs all key-related debug assertions.
#[inline]
pub(crate) fn debug_assert_key(pagesize: usize, flags: DatabaseFlags, key: &[u8]) {
    debug_assert_key_size(pagesize, flags, key);
    debug_assert_integer_key(flags, key);
}

/// Runs all value-related debug assertions.
#[inline]
pub(crate) fn debug_assert_value(pagesize: usize, flags: DatabaseFlags, value: &[u8]) {
    debug_assert_value_size(pagesize, flags, value);
    debug_assert_integer_dup(flags, value);
}

/// Runs all key and value debug assertions for put operations.
#[inline]
pub(crate) fn debug_assert_put(pagesize: usize, flags: DatabaseFlags, key: &[u8], value: &[u8]) {
    debug_assert_key(pagesize, flags, key);
    debug_assert_value(pagesize, flags, value);
}
