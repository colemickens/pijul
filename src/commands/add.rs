/*
  Copyright Florent Becker and Pierre-Etienne Meunier 2015.

  This file is part of Pijul.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
extern crate clap;
use clap::{SubCommand, ArgMatches,Arg};

use commands::StaticSubcommand;
use repository::{Repository,add_file};
use repository::fs_representation::{repo_dir, pristine_dir, find_repo_root};

use std;
use std::io;
use std::fmt;
use std::error;
use std::path::{Path};
use std::fs::{metadata};

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("add")
        .about("add a file to the repository")
        .arg(Arg::with_name("files")
             .multiple(true)
             .help("Files to add to the repository.")
             .required(true)
             );
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Vec<&'a Path> {
    match args.values_of("files") {
        Some(l) => l.iter().map(|p| { Path::new(p.clone()) }).collect(),
        None => vec!()
    }
}

#[derive(Debug)]
pub enum Error <'a>{
    NotInARepository,
    PathNotFound(&'a Path),
    IoError(io::Error),
}

impl <'a> fmt::Display for Error<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::PathNotFound(p) => write!(f, "Path not found: {}", p.to_string_lossy()),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
        }
    }
}

impl <'a> error::Error for Error<'a> {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::PathNotFound(_) => "path not found",
            Error::IoError(ref err) => error::Error::description(err),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::PathNotFound(_) => None,
            Error::NotInARepository => None
        }
    }
}

impl <'a> From<io::Error> for Error<'a> {
    fn from(err: io::Error) -> Error<'a> {
        Error::IoError(err)
    }
}

pub fn run<'a>(files : &Vec<&'a Path>) -> Result<Option<()>, Error<'a>> {
    let pwd = try!(std::env::current_dir());
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            for file in &files[..] {
                match metadata(file) {
                    Ok(_)=>
                        if ! iter_after((pwd.join(*file)).components(), r.components()).is_some() {
                            return Err(Error::NotInARepository)
                        },
                    Err(_) =>
                        return Err(Error::PathNotFound(*file))
                }
            }
            for file in &files[..] {
                match metadata(file) {
                    Ok(m)=> {
                        let mut repo = try!(Repository::new(&repo_dir));
                        add_file(&mut repo,*file,m.is_dir()).unwrap()
                    },
                    Err(_) => {()}
                }
            }
            Ok(None)
        }
    }
}

/// Ce morceau vient de path.rs du projet Rust, sous licence Apache/MIT.
fn iter_after<A, I, J>(mut iter: I, mut prefix: J) -> Option<I> where
    I: Iterator<Item=A> + Clone, J: Iterator<Item=A>, A: PartialEq
{
    loop {
        let mut iter_next = iter.clone();
        match (iter_next.next(), prefix.next()) {
            (Some(x), Some(y)) => {
                if x != y { return None }
            }
            (Some(_), None) => return Some(iter),
            (None, None) => return Some(iter),
            (None, Some(_)) => return None,
        }
        iter = iter_next;
    }
}