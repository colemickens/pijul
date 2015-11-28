#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

extern crate libpijul;
mod commands;

use std::path::Path;

use log::*;
extern crate env_logger;

macro_rules! pijul_subcommand_dispatch {
    ($p:expr => $($subcommand_name:expr => $subcommand:ident),*) => {{
        match $p {
            $(($subcommand_name, Some(args)) =>
             {
                 let params = commands::$subcommand::parse_args(args);
                 match commands::$subcommand::run(&params) {
                     Ok(_) => (),
                     Err(e) => {
                         println!("error: {}", e);
                         std::process::exit(1)
                     }
                 }
             }
              ),*
                ("", None) => {},
            _ => panic!("Incorrect subcommand name")
        }
    }}
}

fn main() {
    env_logger::init().unwrap();
    let app = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            );
    let app = app.subcommands(commands::all_command_invocations());

    let args = app.get_matches();
    pijul_subcommand_dispatch!(args.subcommand() =>
                               "info" => info,
                               "check" => check,
                               "init" => init,
                               "add" => add,
                               "record" => record,
                               "pull" => pull,
                               "get" => get,
                               "remove" => remove,
                               "ls" => ls,
                               "revert" => revert
                               );
}

