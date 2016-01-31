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

use std::path::Path;
use std::path::PathBuf;
use std::fs::{metadata};

use std::io::{BufWriter,BufReader,Read,Write,BufRead};
use std::fs::File;
use std::str::{from_utf8};

use std;
extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha512;
use std::collections::HashSet;
extern crate rand;
extern crate libc;
use self::libc::{memcmp,c_void,size_t};

pub type LocalKey=Vec<u8>;
pub type ExternalKey=Vec<u8>;
pub type ExternalHash=Vec<u8>;
pub type Flag=u8;

use error::Error;

extern crate rustc_serialize;
use self::rustc_serialize::{Encodable,Decodable};
use self::rustc_serialize::hex::ToHex;

extern crate cbor;

use std::collections::BTreeMap;
use super::fs_representation::{patch_path};
use std::process::{Command,Stdio};

#[derive(Debug,Clone,PartialEq,RustcEncodable,RustcDecodable)]
pub enum Value {
    String(String)
}

#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Edge {
    pub from:ExternalKey,
    pub to:ExternalKey,
    pub introduced_by:ExternalHash
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub enum Change {
    NewNodes{
        up_context:Vec<ExternalKey>,
        down_context:Vec<ExternalKey>,
        flag:Flag,
        line_num:u32,
        nodes:Vec<Vec<u8>>
    },
    Edges{ flag:Flag,
           edges:Vec<Edge> }
}

#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Patch {
    pub authors:Vec<BTreeMap<String,Value>>,
    pub name:String,
    pub description:Option<String>,
    pub timestamp:i64,
    pub dependencies:HashSet<ExternalHash>,
    pub changes:Vec<Change>
}

impl Patch {

    pub fn new(authors:Vec<BTreeMap<String,Value>>,name:String,description:Option<String>,timestamp:i64,changes:Vec<Change>)->Patch {
        let deps=dependencies(&changes);
        Patch {
            authors:authors,
            name:name,
            description:description,
            timestamp:timestamp,
            changes:changes,
            dependencies:deps
        }
    }
    pub fn empty()->Patch {
        Patch { authors:vec!(),name:"".to_string(),description:None,timestamp:0,
                changes:vec!(), dependencies:HashSet::new() }
    }

    fn patch_from_file(p:&Path)->Result<Patch,Error> {
        match p.extension().and_then(|x| x.to_str()) {
            Some("gpg") => {
                debug!("starting gpg");
                let mut gpg=
                    try!(Command::new("gpg")
                         .arg("--yes")
                         .arg("--status-fd").arg("2") // report error on stderr.
                         .arg("-d")
                         .arg(p)
                         .stdout(Stdio::piped())
                         .stderr(Stdio::piped())
                         .spawn());
                debug!("gpg started");
                let stat=try!(gpg.wait());
                let stdout = gpg.stdout.take().unwrap();
                let patch=try!(Patch::from_reader(stdout,Some(p)));
                debug!("gpg done");
                if stat.success() {
                    Ok(patch)
                } else {
                    let mut stderr = gpg.stderr.take().unwrap();
                    let mut buf=String::new();
                    stderr.read_to_string(&mut buf);
                    Err(Error::GPG(stat.code().unwrap(),buf))
                }
            },
            Some("cbor") => {
                let mut file=try!(File::open(p));
                Patch::from_reader(&mut file,Some(p))
            },
            _=>{
                // TODO: This should not happen if this function is
                // called from other class methods. Remove this after
                // debugging.
                panic!("Unknown patch extension.")
            }
        }
    }

    pub fn from_repository(p:&Path,i:&[u8])->Result<Patch,Error> {
        if let Some(filename)=patch_path(p,i) {
            Self::patch_from_file(&filename)
        } else {
            Err(Error::PatchNotFound(p.to_path_buf(),i.to_hex()))
        }
    }
    pub fn from_reader<R>(r:R,p:Option<&Path>)->Result<Patch,Error> where R:Read {
        let mut d=cbor::Decoder::from_reader(r);
        if let Some(d)=d.decode().next() {
            Ok(try!(d))
        } else {
            Err(Error::NothingToDecode(p.and_then(|p| Some(p.to_path_buf()))))
        }
    }

    pub fn to_writer<W>(&self,w:&mut W)->Result<(),Error> where W:Write {
        let mut e = cbor::Encoder::from_writer(w);
        try!(self.encode(&mut e));
        //try!(bincode::rustc_serialize::encode_into(self,w,SizeLimit::Infinite).map_err(Error::PatchEncoding));
        Ok(())
    }
    pub fn save(&self,dir:&Path)->Result<Vec<u8>,Error>{
        debug!("saving patch");
        let mut name:[u8;20]=[0;20]; // random name initially
        fn make_name(dir:&Path,name:&mut [u8])->std::path::PathBuf{
            for i in 0..name.len() { let r:u8=rand::random(); name[i] = 97 + (r%26) }
            let tmp=dir.join(std::str::from_utf8(&name[..]).unwrap());
            if std::fs::metadata(&tmp).is_err() { tmp } else { make_name(dir,name) }
        }
        let tmp=make_name(&dir,&mut name);
        {
            let mut buffer = BufWriter::new(try!(File::create(&tmp)));
            try!(self.to_writer(&mut buffer));
        }
        // Sign
        let tmp_gpg=tmp.with_extension("gpg");
        let gpg=Command::new("gpg")
            .arg("--yes")
            .arg("-o")
            .arg(&tmp_gpg)
            .arg("-s")
            .arg(&tmp)
            .spawn();

        // hash
        let mut hasher = Sha512::new();
        {
            let mut buffer = BufReader::new(try!(File::open(&tmp)));
            loop {
                let len= {
                    let buf=try!(buffer.fill_buf());
                    if buf.len()==0 { break } else {
                        hasher.input(buf);buf.len()
                    }
                };
                buffer.consume(len)
            }
        }
        let mut hash=vec![0;hasher.output_bytes()];
        hasher.result(&mut hash);
        if let Ok(true)=gpg.and_then(|mut gpg| {
            let stat=try!(gpg.wait());
            Ok(stat.success())
        }) {
            let mut f=dir.join(hash.to_hex());
            f.set_extension("cbor.gpg");
            try!(std::fs::rename(&tmp_gpg,&f));
            try!(std::fs::remove_file(&tmp));
        } else {
            let mut f=dir.join(hash.to_hex());;
            f.set_extension("cbor");
            try!(std::fs::rename(&tmp,&f));
        }
        Ok(hash)
    }

}


pub fn write_changes(patches:&HashSet<&[u8]>,changes_file:&Path)->Result<(),Error>{
    let file=try!(File::create(changes_file));
    let mut buffer = BufWriter::new(file);
    let mut e = cbor::Encoder::from_writer(&mut buffer);
    try!(patches.encode(&mut e));
    //try!(bincode::rustc_serialize::encode_into(patches,&mut buffer,SizeLimit::Infinite).map_err(Error::PatchEncoding));
    //let encoded=try!(encode(&patches).map_err(Error::Encoder));
    //try!(buffer.write(encoded.as_bytes()).map_err(Error::IO));
    Ok(())
}

pub fn read_changes<R:Read>(r:R,p:Option<&Path>)->Result<HashSet<Vec<u8>>,Error> {
    let mut d=cbor::Decoder::from_reader(r);
    if let Some(d)=d.decode().next() {
        Ok(try!(d))
    } else {
        Err(Error::NothingToDecode(p.and_then(|p| Some(p.to_path_buf()))))
    }
}
pub fn read_changes_from_file(changes_file:&Path)->Result<HashSet<Vec<u8>>,Error> {
    let file=try!(File::open(changes_file));
    let r = BufReader::new(file);
    read_changes(r,Some(changes_file))
}

pub fn dependencies(changes:&[Change])->HashSet<ExternalHash> {
    let mut deps=HashSet::new();
    fn push_dep(deps:&mut HashSet<ExternalHash>,dep:ExternalHash) {
        // don't include ROOT_KEY as a dependency
        debug!(target:"dependencies","dep={}",dep.to_hex());
        if !if dep.len()==HASH_SIZE {unsafe { memcmp(dep.as_ptr() as *const c_void,
                                                     ROOT_KEY.as_ptr() as *const c_void,
                                                     HASH_SIZE as size_t)==0 }} else {false} {
            deps.insert(dep);
        }
    }
    for ch in changes {
        match *ch {
            Change::NewNodes { ref up_context,ref down_context, line_num:_,flag:_,nodes:_ } => {
                for c in up_context.iter().chain(down_context.iter()) {
                    if c.len()>LINE_SIZE { push_dep(&mut deps,c[0..c.len()-LINE_SIZE].to_vec()) }
                }
            },
            Change::Edges{ref edges,..} =>{
                for e in edges {
                    if e.from.len()>LINE_SIZE { push_dep(&mut deps,e.from[0..e.from.len()-LINE_SIZE].to_vec()) }
                    if e.to.len()>LINE_SIZE { push_dep(&mut deps,e.to[0..e.to.len()-LINE_SIZE].to_vec()) }
                    if e.introduced_by.len()>0 { push_dep(&mut deps,e.introduced_by.clone()) }
                }
            }
        }
    }
    deps
}

pub const HASH_SIZE:usize=20; // pub temporaire
pub const LINE_SIZE:usize=4;
pub const KEY_SIZE:usize=HASH_SIZE+LINE_SIZE;
pub const ROOT_KEY:&'static[u8]=&[0;KEY_SIZE];
pub const EDGE_SIZE:usize=1+KEY_SIZE+HASH_SIZE;
