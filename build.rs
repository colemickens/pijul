extern crate gcc;
fn main() {
    gcc::Config::new()
        .file("src/repository/retrieve.c")
        .compile("libretrieve.a");

    println!("cargo:rustc-flags=-l dylib=lmdb");
}
