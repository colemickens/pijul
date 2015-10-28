extern crate libc;
extern crate pijul;
use pijul::repository::*;

fn main() {
    let args = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            (@arg REPOSITORY: "Location of the repository")
            )
        .get_matches();

    let repository = args.value_of("REPOSITORY").unwrap_or("/tmp/test");

    let x=
        with_repository("/tmp/test\0",|_,txn| {
            let rep=open_repository(txn);
            rep
        });
    match x {
        Ok(_)=>println!("ok"),
        Err(e)=>println!("err:{}",e)
    }
}
