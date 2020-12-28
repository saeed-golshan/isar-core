extern crate cc;
extern crate pkg_config;

#[cfg(feature = "bindgen")]
extern crate bindgen;

#[cfg(feature = "bindgen")]
#[path = "bindgen.rs"]
mod generate;

use std::env;
use std::path::PathBuf;
use cc::Build;

fn main() {
    #[cfg(feature = "bindgen")]
    generate::generate();

    let mut lmdb = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    lmdb.push("lmdb");
    lmdb.push("libraries");
    lmdb.push("liblmdb");

    if pkg_config::find_library("liblmdb").is_err() {
        let mut builder = Build::new();

        builder
            .file(lmdb.join("mdb.c"))
            .file(lmdb.join("midl.c"))
            .define("MDB_DEVEL", "2")
            //.define("MDB_DEBUG","2")
            .flag_if_supported("-Wno-unused-parameter")
            .flag_if_supported("-Wbad-function-cast")
            .flag_if_supported("-Wuninitialized");
        //eprintln!("BUILDSIMON");
        builder.compile("liblmdb.a")
    }
}
