use std::{env, path::PathBuf};

#[cfg(feature = "bindgen")]
use std::path::Path;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let mdbx = manifest_dir.join("libmdbx");

    println!("cargo:rerun-if-changed={}", mdbx.display());

    // Binding regeneration requires both the `bindgen` feature AND the
    // `MDBX_REGENERATE_BINDINGS` env var. This prevents accidental regeneration
    // when running `cargo build --all-features` on the workspace.
    //
    // To regenerate bindings intentionally:
    //   MDBX_REGENERATE_BINDINGS=1 cargo build -p signet-mdbx-sys --features bindgen
    #[cfg(feature = "bindgen")]
    if env::var("MDBX_REGENERATE_BINDINGS").is_ok() {
        let bindings = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
        generate_bindings(&mdbx, &bindings);

        // Also write to src/bindings.rs for easy commit
        let src_bindings = manifest_dir.join("src").join("bindings.rs");
        std::fs::copy(&bindings, &src_bindings).expect("Failed to copy bindings to src/");
        println!("cargo:warning=Regenerated bindings. Don't forget to commit src/bindings.rs");
    }

    let mut cc = cc::Build::new();
    cc.flag_if_supported("-Wno-unused-parameter").flag_if_supported("-Wuninitialized");

    if env::var("CARGO_CFG_TARGET_OS").unwrap() != "linux" {
        cc.flag_if_supported("-Wbad-function-cast");
    }

    let flags = format!("{:?}", cc.get_compiler().cflags_env());
    cc.define("MDBX_BUILD_FLAGS", flags.as_str()).define("MDBX_TXN_CHECKOWNER", "0");

    // Enable debugging on debug builds
    #[cfg(debug_assertions)]
    cc.define("MDBX_DEBUG", "1").define("MDBX_ENABLE_PROFGC", "1");

    // Disables debug logging on optimized builds
    #[cfg(not(debug_assertions))]
    cc.define("MDBX_DEBUG", "0").define("NDEBUG", None);

    // Propagate `-C target-cpu=native`
    let rustflags = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap();
    if rustflags.contains("target-cpu=native")
        && env::var("CARGO_CFG_TARGET_ENV").unwrap() != "msvc"
    {
        cc.flag("-march=native");
    }

    cc.file(mdbx.join("mdbx.c")).compile("libmdbx.a");
}

#[cfg(feature = "bindgen")]
fn generate_bindings(mdbx: &Path, out_file: &Path) {
    use bindgen::{
        Formatter,
        callbacks::{IntKind, ParseCallbacks},
    };

    #[derive(Debug)]
    struct Callbacks;

    impl ParseCallbacks for Callbacks {
        fn int_macro(&self, name: &str, _value: i64) -> Option<IntKind> {
            match name {
                "MDBX_SUCCESS"
                | "MDBX_KEYEXIST"
                | "MDBX_NOTFOUND"
                | "MDBX_PAGE_NOTFOUND"
                | "MDBX_CORRUPTED"
                | "MDBX_PANIC"
                | "MDBX_VERSION_MISMATCH"
                | "MDBX_INVALID"
                | "MDBX_MAP_FULL"
                | "MDBX_DBS_FULL"
                | "MDBX_READERS_FULL"
                | "MDBX_TLS_FULL"
                | "MDBX_TXN_FULL"
                | "MDBX_CURSOR_FULL"
                | "MDBX_PAGE_FULL"
                | "MDBX_MAP_RESIZED"
                | "MDBX_INCOMPATIBLE"
                | "MDBX_BAD_RSLOT"
                | "MDBX_BAD_TXN"
                | "MDBX_BAD_VALSIZE"
                | "MDBX_BAD_DBI"
                | "MDBX_LOG_DONTCHANGE"
                | "MDBX_DBG_DONTCHANGE"
                | "MDBX_RESULT_TRUE"
                | "MDBX_UNABLE_EXTEND_MAPSIZE"
                | "MDBX_PROBLEM"
                | "MDBX_LAST_LMDB_ERRCODE"
                | "MDBX_BUSY"
                | "MDBX_EMULTIVAL"
                | "MDBX_EBADSIGN"
                | "MDBX_WANNA_RECOVERY"
                | "MDBX_EKEYMISMATCH"
                | "MDBX_TOO_LARGE"
                | "MDBX_THREAD_MISMATCH"
                | "MDBX_TXN_OVERLAPPING"
                | "MDBX_LAST_ERRCODE" => Some(IntKind::Int),
                _ => Some(IntKind::UInt),
            }
        }
    }

    let bindings = bindgen::Builder::default()
        .header(mdbx.join("mdbx.h").to_string_lossy())
        .allowlist_var("^(MDBX|mdbx)_.*")
        .allowlist_type("^(MDBX|mdbx)_.*")
        .allowlist_function("^(MDBX|mdbx)_.*")
        .size_t_is_usize(true)
        .merge_extern_blocks(true)
        .parse_callbacks(Box::new(Callbacks))
        .layout_tests(false)
        .prepend_enum_name(false)
        .generate_comments(false)
        .formatter(Formatter::Rustfmt)
        .generate()
        .expect(
            "Unable to generate bindings. Ensure LIBCLANG_PATH is set.\n\
             On macOS: export LIBCLANG_PATH=$(brew --prefix llvm)/lib\n\
                       export DYLD_FALLBACK_LIBRARY_PATH=$(brew --prefix llvm)/lib\n\
             On Linux: apt-get install libclang-dev",
        );
    bindings.write_to_file(out_file).expect("Couldn't write bindings!");
}
