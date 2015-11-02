extern crate libc;
#[macro_use]
extern crate clap;

extern crate pijul;

fn main() {
    let app = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            );
    let app = app.subcommands(pijul::commands::all_command_invocations());

    let args = app.get_matches();

    match args.subcommand() {
        ("info", Some(info_args)) =>
        {
            let request = pijul::commands::info::parse_args(info_args);
            pijul::commands::info::run(&request)
        },
        ("check", Some(check_args)) =>
        {
            let repository = pijul::commands::check::parse_args(check_args);
            match pijul::commands::check::run(&repository) {
                Ok(()) => (),
                Err(e) => {
                    println!("err: {}", e);
                    std::process::exit(1)
                }
            }
        },

        ("", None) =>
        {
            let repository = pijul::commands::check::CheckArgs
                                   {repository : "/tmp/test\0"};
            pijul::commands::check::run(&repository).unwrap()
        }
        _ => panic!("Incorrect subcommand name"),
    }
}

