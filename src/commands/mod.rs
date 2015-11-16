extern crate clap;

pub type StaticSubcommand = clap::App<'static, 'static, 'static, 'static, 'static, 'static>;

pub mod info;
pub mod check;
pub mod init;
pub mod record;
pub mod add;
pub mod pull;
pub mod get;
mod test;

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![
        check::invocation(),
        info::invocation(),
        init::invocation(),
        record::invocation(),
        add::invocation(),
        pull::invocation(),
        get::invocation()
        ];
}

