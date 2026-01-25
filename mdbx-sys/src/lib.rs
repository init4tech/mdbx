//! [`libmdbx`](https://github.com/erthink/libmdbx) bindings.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case, clippy::all)]

#[cfg(target_os = "linux")]
mod bindings_linux;
#[cfg(target_os = "linux")]
pub use bindings_linux::*;

#[cfg(target_os = "windows")]
mod bindings_windows;
#[cfg(target_os = "windows")]
pub use bindings_windows::*;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod bindings_macos;
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub use bindings_macos::*;
