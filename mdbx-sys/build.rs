use std::{env, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let mdbx = manifest_dir.join("libmdbx");

    println!("cargo:rerun-if-changed={}", mdbx.display());

    let mut cc = cc::Build::new();
    cc.flag_if_supported("-Wno-unused-parameter").flag_if_supported("-Wuninitialized");

    if env::var("CARGO_CFG_TARGET_OS").unwrap() != "linux" {
        cc.flag_if_supported("-Wbad-function-cast");
    }

    let flags = format!("{:?}", cc.get_compiler().cflags_env());
    cc.define("MDBX_BUILD_FLAGS", flags.as_str()).define("MDBX_TXN_CHECKOWNER", "0");

    #[cfg(debug_assertions)]
    cc.define("MDBX_DEBUG", "1").define("MDBX_ENABLE_PROFGC", "1");

    #[cfg(not(debug_assertions))]
    cc.define("MDBX_DEBUG", "0").define("NDEBUG", None);

    let rustflags = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    if rustflags.contains("target-cpu=native")
        && env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default() != "msvc"
    {
        cc.flag("-march=native");
    }

    cc.file(mdbx.join("mdbx.c")).compile("libmdbx.a");
}
