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
extern crate libc;
extern crate time;
extern crate serde;
#[macro_use]
extern crate log;

mod lmdb;

use self::libc::{memcmp, c_void, size_t, c_char};
use std::ptr::{copy_nonoverlapping};
use std::ptr;
use std::sync::Arc;
use std::thread;

use std::str;

use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::path::{PathBuf,Path};

use std::io::{Write,BufRead,Read};
use std::collections::HashSet;
use std::fs::{metadata};

pub mod contents;
use self::contents::{Inode, Line, Graph, LineBuffer};
use self::contents::{PSEUDO_EDGE, FOLDER_EDGE, PARENT_EDGE, DELETED_EDGE, LINE_HALF_DELETED};
use self::contents::{LINE_ONSTACK, LINE_VISITED, DIRECTORY_FLAG};

pub mod fs_representation;
use self::fs_representation::*;
pub mod patch;
use self::patch::*;

pub mod error;
use self::error::Error;

use std::collections::BTreeSet;
#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;

use std::fs;

extern crate rand;

use std::marker::PhantomData;
use std::io::{BufWriter,BufReader};
use std::fs::File;

/// The repository structure, on which most functions work.
pub struct Repository<'a> {
    mdb_env:lmdb::Env,
    mdb_txn:lmdb::Txn<'a>,
    dbi_nodes:lmdb::Dbi,
    dbi_revdep:lmdb::Dbi,
    dbi_contents:lmdb::Dbi,
    dbi_internal:lmdb::Dbi,
    dbi_external:lmdb::Dbi,
    dbi_branches:lmdb::Dbi,
    dbi_tree:lmdb::Dbi,
    dbi_revtree:lmdb::Dbi,
    dbi_inodes:lmdb::Dbi,
    dbi_revinodes:lmdb::Dbi
}

impl <'a>Drop for Repository<'a> {
    fn drop(&mut self){
        unsafe {
            if std::thread::panicking() {
                self.mdb_txn.unsafe_abort()
            } else {
                self.mdb_txn.unsafe_commit().unwrap()
            }
        }
    }
}


const INODE_SIZE:usize=16;
/// The size of internal patch id. Allocate a buffer of this size when calling e.g. apply.
const ROOT_INODE:&'static[u8]=&[0;INODE_SIZE];
/// The name of the default branch, "main".
pub const DEFAULT_BRANCH:&'static str="main";

struct Diff<'a> {
    lines_a:Vec<&'a[u8]>,
    contents_a:Vec<&'a[u8]>
}

impl <'a> LineBuffer<'a> for Diff<'a> {
    fn output_line(&mut self,k:&'a[u8],c:&'a[u8]) {
        //println!("outputting {:?} {}",k,unsafe {std::str::from_utf8_unchecked(c)});
        self.lines_a.push(k);
        self.contents_a.push(c);
    }
}

impl <'a,W> LineBuffer<'a> for W where W:std::io::Write {
    fn output_line(&mut self,_:&'a[u8],c:&'a[u8]) {
        self.write(c).unwrap(); // .expect("output_line: could not write");
    }
}


struct CursIter<'a,'b> {
    cursor: *mut lmdb::MdbCursor,
    op:lmdb::Op,
    edge_flag:u8,
    include_pseudo:bool,
    include_folder:bool,
    key:&'b[u8],
    marker:PhantomData<&'a()>
}

impl <'a,'b>CursIter<'a,'b> {
    fn new(curs:*mut lmdb::MdbCursor,key:&'b [u8],flag:u8,include_folder:bool,include_pseudo:bool)->CursIter<'a,'b>{
        CursIter { cursor:curs,
                   key:key,
                   include_pseudo:include_pseudo,
                   include_folder:include_folder,
                   edge_flag:flag,
                   op:lmdb::Op::MDB_GET_BOTH_RANGE,
                   marker:PhantomData }
    }
}

impl <'a,'b>Iterator for CursIter<'a,'b> {
    type Item=&'a [u8];
    fn next(&mut self)->Option<&'a[u8]>{
        match unsafe { lmdb::cursor_get(self.cursor,self.key,Some(&[self.edge_flag][..]),
                                        std::mem::replace(&mut self.op, lmdb::Op::MDB_NEXT_DUP)) } {
            Ok((_,val))=> {
                if val[0] >= self.edge_flag && val[0] <= self.edge_flag | (PSEUDO_EDGE|FOLDER_EDGE)
                    && (self.include_pseudo || val[0]&PSEUDO_EDGE == 0)
                    && (self.include_folder || val[0]&FOLDER_EDGE == 0) {
                        Some(val)
                } else {
                    None
                }
            },
            Err(_) => None
        }
    }
}

#[cfg(not(windows))]
fn permissions(attr:&std::fs::Metadata)->usize{
    attr.permissions().mode() as usize
}
#[cfg(windows)]
fn permissions(attr:&std::fs::Metadata)->usize{
    0
}

fn has_edge(cursor:&mut lmdb::MdbCursor,key:&[u8],flag0:u8,include_folder:bool,include_pseudo:bool)->bool {
    let mut flag=[flag0];
    debug!(target:"has_edge", "{:?}",flag[0]);
    while flag[0] <= flag0|PSEUDO_EDGE|FOLDER_EDGE {
        if (flag[0] & PSEUDO_EDGE != 0 && include_pseudo)
            || (flag[0] & FOLDER_EDGE !=0 && include_folder)
            || (flag[0] == flag0)
        {
            match unsafe {lmdb::cursor_get(cursor,&key,Some(&flag[..]),lmdb::Op::MDB_GET_BOTH_RANGE)} {
                Ok((_,v))=>{
                    debug_assert!(v.len()>=1);
                    debug!(target:"has_edge", "{:?} == {:?} ?",flag[0],v[0]);
                    if v[0]==flag[0] { return true }
                },
                _=>{}
            }
        }
        flag[0]+=1
    }
    false
}



impl <'a> Repository<'a> {
    pub fn new(path:&std::path::Path)->Result<Repository<'a>,Error>{
        let env=try!(lmdb::Env_::new());
        let _=try!(env.reader_check());
        try!(env.set_maxdbs(10));
        try!(env.set_mapsize( (1 << 30) ));
        let env=try!(env.open(path,0,0o755));
        unsafe {
            let txn=try!(env.unsafe_txn(0));
            let dbi_nodes=try!(txn.unsafe_dbi_open(b"nodes\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT|lmdb::MDB_DUPFIXED));
            let dbi_revdep=try!(txn.unsafe_dbi_open(b"revdep\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT));
            let dbi_contents=try!(txn.unsafe_dbi_open(b"contents\0",lmdb::MDB_CREATE));
            let dbi_internal=try!(txn.unsafe_dbi_open(b"internal\0",lmdb::MDB_CREATE));
            let dbi_external=try!(txn.unsafe_dbi_open(b"external\0",lmdb::MDB_CREATE));
            let dbi_branches=try!(txn.unsafe_dbi_open(b"branches\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT));
            let dbi_tree=try!(txn.unsafe_dbi_open(b"tree\0",lmdb::MDB_CREATE));
            let dbi_revtree=try!(txn.unsafe_dbi_open(b"revtree\0",lmdb::MDB_CREATE));
            let dbi_inodes=try!(txn.unsafe_dbi_open(b"inodes\0",lmdb::MDB_CREATE));
            let dbi_revinodes=try!(txn.unsafe_dbi_open(b"revinodes\0",lmdb::MDB_CREATE));
            let repo=Repository{
                mdb_env:env,
                mdb_txn:txn,
                dbi_nodes:dbi_nodes,
                dbi_revdep:dbi_revdep,
                dbi_contents:dbi_contents,
                dbi_internal:dbi_internal,
                dbi_external:dbi_external,
                dbi_branches:dbi_branches,
                dbi_tree:dbi_tree,
                dbi_revtree:dbi_revtree,
                dbi_inodes:dbi_inodes,
                dbi_revinodes:dbi_revinodes
            };
            Ok(repo)
        }
    }



    fn create_new_inode(&mut self,buf:&mut [u8]){
        let curs_revtree=self.mdb_txn.cursor(self.dbi_revtree).unwrap();
        for i in 0..INODE_SIZE { buf[i]=rand::random() }
        while let Ok((_,x))=curs_revtree.get(&buf,None,lmdb::Op::MDB_SET_RANGE) {
            if unsafe { memcmp(buf.as_ptr() as *const c_void,
                               x.as_ptr() as *const c_void,
                               INODE_SIZE as size_t) } != 0 {
                break
            } else {
                for i in 0..INODE_SIZE { buf[i]=rand::random() }
            }
        }
    }

    fn add_inode(&mut self, inode:&Option<&[u8]>, path:&std::path::Path, is_dir:bool)->Result<(),Error>{
        let mut buf:Inode=vec![0;INODE_SIZE];
        let mut components=path.components();
        let mut cs=components.next();
        while let Some(s)=cs { // need to peek at the next element, so no for.
            cs=components.next();
            let ss=s.as_os_str().to_str().unwrap();
            buf.truncate(INODE_SIZE);
            buf.extend(ss.as_bytes());
            let mut broken=false;
            {
                debug!(target:"mv","mdb_get: dbi_tree, {}",to_hex(&buf));
                match self.mdb_txn.get(self.dbi_tree,&buf) {
                    Ok(Some(v))=> {
                        debug!(target:"mv","got Some({})",to_hex(v));
                        if cs.is_none() {
                            return Err(Error::AlreadyAdded)
                        } else {
                            // replace buf with existing inode
                            buf.clear();
                            buf.extend(v);
                        }
                    },
                    _ =>{
                        broken=true
                    }
                }
            }
            if broken {
                let mut inode_:[u8;INODE_SIZE]=[0;INODE_SIZE];
                let inode = if cs.is_none() && inode.is_some() {
                    inode.unwrap()
                } else {
                    self.create_new_inode(&mut inode_);
                    &inode_[..]
                };
                debug!(target:"mv","put: dbi_tree, {} {}",to_hex(&buf),to_hex(&inode));
                self.mdb_txn.put(self.dbi_tree,&buf,&inode,0).unwrap();
                self.mdb_txn.put(self.dbi_revtree,&inode,&buf,0).unwrap();
                if cs.is_some() || is_dir {
                    self.mdb_txn.put(self.dbi_tree,&inode,&[],0).unwrap();
                }
                // push next inode onto buf.
                buf.clear();
                buf.extend(inode)
            }
        }
        Ok(())
    }
    /// Adds a file in the repository. Additions need to be recorded in
    /// order to produce a patch.
    pub fn add_file(&mut self, path:&std::path::Path, is_dir:bool)->Result<(),Error>{
        self.add_inode(&None,path,is_dir)
    }


    pub fn move_file(&mut self, path:&std::path::Path, path_:&std::path::Path,is_dir:bool) -> Result<(), Error>{
        debug!(target:"mv","move_file: {:?},{:?}",path,path_);
        let inode= &mut (Vec::new());
        let parent= &mut (Vec::new());

        (*inode).extend(ROOT_INODE);
        for c in path.components() {
            inode.truncate(INODE_SIZE);
            inode.extend(c.as_os_str().to_str().unwrap().as_bytes());
            debug!(target:"mv","first get: {}",to_hex(inode));
            match self.mdb_txn.get(self.dbi_tree,&inode) {
                Ok(Some(x))=> {
                    debug!(target:"mv","got some: {}",to_hex(x));
                    std::mem::swap(inode,parent);
                    (*inode).clear();
                    (*inode).extend(x);
                },
                _=>{
                    debug!(target:"mv","got none");
                    return Err(Error::FileNotInRepo(path.to_path_buf()))
                }
            }
        }
        // Now the last inode is in "*inode"
        debug!(target:"mv","mdb_txn.del parent={:?}",to_hex(parent));
        try!(self.mdb_txn.del(self.dbi_tree,parent,None));

        let basename=path.file_name().unwrap();
        (*parent).truncate(INODE_SIZE);
        (*parent).extend(basename.to_str().unwrap().as_bytes());

        debug!(target:"mv","inode={} path_={:?}",to_hex(inode),path_);
        try!(self.add_inode(&Some(inode),path_,is_dir));
        let vv=
            match self.mdb_txn.get(self.dbi_inodes,inode) {
                Ok(Some(v))=> {
                    let mut vv=v.to_vec();
                    vv[0]=1;
                    Some(vv)
                },
                _=>None
            };
        if let Some(vv)=vv {
            self.mdb_txn.put(self.dbi_inodes,inode,&vv,0).unwrap();
        };
        Ok(())
    }
    pub fn remove_file(&mut self, path:&std::path::Path) -> Result<(), Error>{
        let mut inode=Vec::new();
        inode.extend(ROOT_INODE);
        let mut comp=path.components();
        let mut c=comp.next();
        loop {
            match c {
                Some(sc)=>{
                    //println!("inode {} + {:?}",to_hex(&inode),sc);
                    inode.extend(sc.as_os_str().to_str().unwrap().as_bytes());
                    match self.mdb_txn.get(self.dbi_tree,&inode) {
                        Ok(Some(x))=> { c=comp.next();
                                  if c.is_some() {inode.clear(); inode.extend(x) }
                        },
                        _ => return Err(Error::FileNotInRepo(path.to_path_buf()))
                    }
                },
                _=>break
            }
        }
        // This function returns a boolean indicating whether the directory we are trying to delete is non-empty, and deletes it if so.
        fn rec_delete(repo:&mut Repository,key:&[u8])->bool {
            //println!("rec_delete {}",to_hex(key));
            let mut children=Vec::new();
            // First, kill the inode itself, if it exists (or mark it deleted)
            //let mut k = MDB_val{ mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            //let mut v : MDB_val = std::mem::zeroed();
            {
                let curs=repo.mdb_txn.cursor(repo.dbi_tree).unwrap();
                let mut result=curs.get(&key,None,lmdb::Op::MDB_SET_RANGE);
                loop {
                    match result {
                        Ok((k,v))=>
                            if unsafe { memcmp(k.as_ptr() as *const c_void,key.as_ptr() as *const c_void,
                                               key.len() as size_t) } ==0 {
                                //debug_assert!(v.mv_size as usize==INODE_SIZE);
                                if v.len()>0 {
                                    children.push((k.to_vec(),v.to_vec()));
                                }
                                result=curs.get(&k,Some(&v),lmdb::Op::MDB_NEXT);
                            } else {
                                break
                            },
                        _=>break
                    }
                }
            }
            {
                for (a,b) in children {
                    if rec_delete(repo,&b) {
                        //println!("deleting {} {}",to_hex(&a),to_hex(&b));
                        repo.mdb_txn.del(repo.dbi_tree,&a,Some(&b)).unwrap();
                        repo.mdb_txn.del(repo.dbi_revtree,&b,Some(&a)).unwrap();
                    }
                }
            }
            let mut node_=[0;3+KEY_SIZE];
            // If the directory is empty, then mark the corresponding node as deleted (flag '2').
            // TODO: this could be done by unsafely mutating the lmdb memory.
            let b=
                match repo.mdb_txn.get(repo.dbi_inodes,key) {
                    Ok(Some(node)) => {
                        debug!(target:"remove_file","node={}",to_hex(node));
                        debug_assert!(node.len()==3+KEY_SIZE);
                        unsafe {
                            copy_nonoverlapping(node.as_ptr() as *const c_void,
                                                node_.as_ptr() as *mut c_void,
                                                3+KEY_SIZE);
                        }
                        node_[0]=2;
                        false
                    },
                    Ok(None)=>true,
                    Err(_)=>{
                        panic!("delete panic")
                    }
                };
            if !b {
                repo.mdb_txn.put(repo.dbi_inodes,key,&node_[..],0).unwrap();
            }
            b
        }
        rec_delete(self,&inode);
        Ok(())
    }


    pub fn list_files(&self)->Vec<PathBuf>{
        fn collect(repo:&Repository,key:&[u8],pb:&Path, basename:&[u8],files:&mut Vec<PathBuf>) {
            //println!("collecting {:?},{:?}",to_hex(key),std::str::from_utf8_unchecked(basename));
            let add= match repo.mdb_txn.get(repo.dbi_inodes,key) {
                Ok(Some(node)) => node[0]<2,
                Ok(None)=> true,
                Err(_)=>panic!("list_files panic")
            };
            if add {
                let next_pb=pb.join(std::str::from_utf8(basename).unwrap());
                let next_pb_=next_pb.clone();
                if basename.len()>0 { files.push(next_pb) }
                let curs=repo.mdb_txn.cursor(repo.dbi_tree).unwrap();

                let mut result = curs.get(key,None,lmdb::Op::MDB_SET_RANGE);
                loop {
                    match result {
                        Ok((k,v))=>{
                            if v.len()>0 && unsafe {memcmp(k.as_ptr() as *const c_void,
                                                           key.as_ptr() as *const c_void,
                                                           INODE_SIZE as size_t) }==0 {

                                collect(repo,v,next_pb_.as_path(),&k[INODE_SIZE..],files);
                                result=curs.get(key,Some(v),lmdb::Op::MDB_NEXT_DUP);
                            } else {
                                break
                            }
                        },
                        _=>break
                    }
                }
            }
        }
        let mut files=Vec::new();
        let mut pathbuf=PathBuf::new();
        collect(self,&ROOT_INODE[..], &mut pathbuf, &[], &mut files);
        files
    }


    pub fn get_current_branch(&'a self)->&'a[u8] {
        match self.mdb_txn.get(self.dbi_branches,&[0]) {
            Ok(Some(b))=>b,
            Ok(None)=>DEFAULT_BRANCH.as_bytes(),
            Err(_)=>panic!("get_current_branch")
        }
    }



    fn retrieve(&'a self,key:&'a [u8])->Result<Graph<'a>,()>{
        fn retr<'a,'b,'c>(cache:&mut HashMap<&'a [u8],usize>,
                          curs:&'b mut lmdb::MdbCursor,
                          lines:&mut Vec<Line<'a>>,
                          children:&mut Vec<(*const u8,usize)>,
                          key:&'a[u8])->usize {
            {
                match cache.entry(key) {
                    Entry::Occupied(e) => return *(e.get()),
                    Entry::Vacant(e) => {
                        let idx=lines.len();
                        e.insert(idx);
                        debug!(target:"retrieve","{}",to_hex(key));
                        let is_zombie={
                            let mut tag=PARENT_EDGE|DELETED_EDGE;
                            unsafe {
                                (match lmdb::cursor_get(curs,key,Some(&[tag][..]),lmdb::Op::MDB_GET_BOTH_RANGE) {
                                    Ok((_,v)) if v[0]==tag => true,
                                    _=>false
                                }) ||
                                    ({tag=PARENT_EDGE|DELETED_EDGE|FOLDER_EDGE;
                                      match lmdb::cursor_get(curs,key,Some(&[tag][..]),lmdb::Op::MDB_GET_BOTH_RANGE) {
                                          Ok((_,v)) if v[0]==tag => true,
                                          _=>false
                                      }})
                            }
                        };
                        let mut l=Line {
                            key:key,flags:if is_zombie {LINE_HALF_DELETED} else {0},
                            children:children.len(),n_children:0,index:0,lowlink:0,scc:0
                        };
                        for child in CursIter::new(curs,key,0,true,true) {
                            debug!(target:"retrieve", "child: {}",to_hex(child));
                            children.push((child.as_ptr(),0));
                            l.n_children+=1
                        }
                        lines.push(l)
                    }
                }
            }
            let idx=lines.len()-1;
            let l_children=lines[idx].children;
            let n_children=lines[idx].n_children;
            debug!(target:"retrieve", "n_children: {}",n_children);
            for i in 0..n_children {
                let (a,_)=children[l_children+i];
                let child_key = unsafe {
                    std::slice::from_raw_parts(a.offset(1),KEY_SIZE)
                };
                children[l_children+i] = (a, retr(cache,curs,lines,children,child_key))
            }
            if n_children==0 {
                children.push((std::ptr::null(),0));
                lines[idx].n_children=1;
            }
            idx
        }
        let mut cache=HashMap::new();
        let mut cursor= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let mut lines=Vec::new();
        // Insert last line (so that all lines have a common descendant).
        lines.push(Line {
            key:&b""[..],flags:0,children:0,n_children:0,index:0,lowlink:0,scc:0
        });
        cache.insert(&b""[..],0);
        let mut children=Vec::new();
        retr(&mut cache,cursor,&mut lines,&mut children,key);
        unsafe { lmdb::mdb_cursor_close(cursor) };
        Ok(Graph { lines:lines, children:children })
    }

    pub fn contents<'b>(&'b self,key:&[u8]) -> &'b[u8] {
        debug_assert!(key.len() == KEY_SIZE);
        match self.mdb_txn.get(self.dbi_contents,key) {
            Ok(Some(v))=>v,
            Ok(None)=>&[],
            Err(e) =>{
                debug!("contents error for key {}",to_hex(key));
                panic!("contents error: {:?}", e)
            }
        }
    }


    fn tarjan(&self,line:&mut Graph)->Vec<Vec<usize>> {
        fn dfs<'a>(repo:&Repository,
                   scc:&mut Vec<Vec<usize>>,
                   stack:&mut Vec<usize>,
                   index:&mut usize, g:&mut Graph<'a>, n_l:usize){
            {
                let mut l=&mut (g.lines[n_l]);
                (*l).index = *index;
                (*l).lowlink = *index;
                (*l).flags |= LINE_ONSTACK | LINE_VISITED;
                debug!(target:"tarjan", "{} {} chi",to_hex((*l).key),(*l).n_children);
                //unsafe {println!("contents: {}",std::str::from_utf8_unchecked(repo.contents((*l).key))); }
            }
            stack.push(n_l);
            *index = *index + 1;
            for i in 0..g.lines[n_l].n_children {
                //let mut l=&mut (g.lines[n_l]);

                let (_,n_child) = g.children[g.lines[n_l].children + i];
                //println!("children: {}",to_hex(g.lines[n_child].key));

                if g.lines[n_child].flags & LINE_VISITED == 0 {
                    dfs(repo,scc,stack,index,g,n_child);
                    g.lines[n_l].lowlink=std::cmp::min(g.lines[n_l].lowlink, g.lines[n_child].lowlink);
                } else {
                    if g.lines[n_child].flags & LINE_ONSTACK != 0 {
                        g.lines[n_l].lowlink=std::cmp::min(g.lines[n_l].lowlink, g.lines[n_child].index)
                    }
                }
            }

            if g.lines[n_l].index == g.lines[n_l].lowlink {
                //println!("SCC: {:?}",slice::from_raw_parts((*l).key,KEY_SIZE));
                let mut v=Vec::new();
                loop {
                    match stack.pop() {
                        None=>break,
                        Some(n_p)=>{
                            g.lines[n_p].scc= scc.len();
                            g.lines[n_p].flags = g.lines[n_p].flags ^ LINE_ONSTACK;
                            v.push(n_p);
                            if n_p == n_l { break }
                        }
                    }
                }
                scc.push(v);
                //*scc+=1
            }
        }
        let mut stack=vec!();
        let mut index=0;
        let mut scc=Vec::with_capacity(line.lines.len());
        //let mut scc=0;
        dfs(self,&mut scc, &mut stack, &mut index, line, 1);
        scc
    }





    fn output_file<'b,'c:'b,B:LineBuffer<'c>>(&'c self,buf:&'b mut B,g:Graph<'a>,forward:&mut Vec<u8>) {
        debug!(target:"conflict","output_file");
        let mut g=g;
        let t0=time::precise_time_s();
        let mut scc = self.tarjan(&mut g); // SCCs are given here in reverse order.
        let t1=time::precise_time_s();
        info!("tarjan took {}s",t1-t0);
        info!("There are {} SCC",scc.len());
        //let mut levels=vec![0;scc];
        let mut last_visit=vec![0;scc.len()];
        let mut first_visit=vec![0;scc.len()];
        let mut step=1;
        fn dfs<'a>(g:&mut Graph<'a>,
                   first_visit:&mut[usize],
                   last_visit:&mut[usize],
                   forward:&mut Vec<u8>,
                   zero:&[u8],
                   step:&mut usize,
                   scc:&[Vec<usize>],
                   mut n_scc:usize) {
            let mut child_components=BTreeSet::new();
            let mut skipped=vec!(n_scc);
            loop {
                first_visit[n_scc] = *step;
                debug!(target:"output_file","step={} scc={}",*step,n_scc);
                *step += 1;
                child_components.clear();
                let mut next_scc=0;
                for cousin in scc[n_scc].iter() {
                    debug!(target:"output_file","cousin: {}",*cousin);
                    let n=g.lines[*cousin].n_children;
                    for i in 0 .. n {
                        let (_,n_child) = g.children[g.lines[*cousin].children + i];
                        let child_component=g.lines[n_child].scc;
                        if child_component < n_scc { // if this is a child and not a sibling.
                            child_components.insert(child_component);
                            next_scc=child_component
                        }
                    }
                }
                if child_components.len() != 1 { break } else {
                    n_scc=next_scc;
                    skipped.push(next_scc);
                }
            }
            let mut forward_scc=HashSet::new();
            for component in child_components.iter().rev() {
                if first_visit[*component] > first_visit[n_scc] { // forward edge
                    debug!(target:"output_file","forward ! {} {}",n_scc,*component);
                    forward_scc.insert(*component);
                } else {
                    debug!(target:"output_file","visiting scc {} {}",*component,to_hex(g.lines[scc[*component][0]].key));
                    dfs(g,first_visit,last_visit,forward,zero,step,scc,*component)
                }
            }
            for cousin in scc[n_scc].iter() {
                let n=g.lines[*cousin].n_children;
                for i in 0 .. n {
                    let (flag_child,n_child) = g.children[g.lines[*cousin].children + i];
                    let child_component=g.lines[n_child].scc;
                    let is_forward=forward_scc.contains(&child_component);
                    if is_forward {
                        if unsafe {*flag_child} & 1 != 0 {
                            forward.push(PSEUDO_EDGE|PARENT_EDGE);
                            forward.extend(g.lines[*cousin].key);
                            forward.extend(zero);
                            forward.push(PSEUDO_EDGE);
                            forward.extend(g.lines[n_child].key);
                        }
                        // Indicate here that we do not want to follow this edge (it is forward).
                        let (a,_)=g.children[g.lines[*cousin].children+i];
                        g.children[g.lines[*cousin].children + i] = (a,0);
                    }
                }
            }
            for i in skipped.iter().rev() {
                last_visit[*i] = *step;
                *step+=1;
            }
        }
        let zero=[0;HASH_SIZE];
        dfs(&mut g,&mut first_visit,&mut last_visit,forward,&zero[..],&mut step,&scc,scc.len()-1);
        debug!("dfs done");
        // assumes no conflict for now.
        let mut i=scc.len()-1;
        let mut nodes=vec!();
        let mut selected_zombies=HashMap::new();
        let cursor= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        loop {
            // test for conflict
            // scc[i] has at least one element (from tarjan).
            if scc[i].len() == 1 && first_visit[i] <= first_visit[0] && last_visit[i] >= last_visit[0]  && g.lines[scc[i][0]].flags & LINE_HALF_DELETED == 0 {
                debug!(target:"conflict","/flag = {} {}",g.lines[scc[i][0]].flags,LINE_HALF_DELETED);
                let key=g.lines[scc[i][0]].key;
                debug!(target:"conflict","key = {}",to_hex(key));
                if key.len()>0 {
                    buf.output_line(&key,self.contents(key));
                }
                if i==0 { break } else { i-=1 }
            } else {
                debug!(target:"conflict","flag = {} {}",g.lines[scc[i][0]].flags,LINE_HALF_DELETED);
                let key=g.lines[scc[i][0]].key;
                debug!(target:"conflict","key = {}",to_hex(key));
                struct A<'b,'a:'b,'c,B:LineBuffer<'a>> where 'a:'c, B:'b {
                    repo:&'a Repository<'a>,
                    scc:&'c mut Vec<Vec<usize>>,
                    first_visit:&'c[usize],
                    last_visit:&'c[usize],
                    selected_zombies:&'c mut HashMap<&'a [u8],bool>,
                    g:&'b Graph<'a>,
                    b:&'b mut B,
                    nodes:&'c mut Vec<&'a[u8]>,
                    next:usize,
                    is_first:bool,
                    cursor:*mut lmdb::MdbCursor
                }
                fn get_conflict<'b,'a:'b,'c,B:LineBuffer<'a>>(x:&mut A<'b,'a,'c,B>,i:usize) {
                    // x.scc[i] has at least one element (from tarjan).
                    {
                        let key=x.g.lines[x.scc[i][0]].key;
                        debug!(target:"conflict","get_conflict: {} {}",x.scc[i][0],to_hex(key));
                    }
                    if x.scc[i].len() == 1 && x.first_visit[i] <= x.first_visit[0] && x.last_visit[i] >= x.last_visit[0] && x.g.lines[x.scc[i][0]].flags & LINE_HALF_DELETED == 0 {
                        // End of conflict.
                        debug!(target:"conflict","end of conflict");
                        let mut first=false; // Detect the first line
                        for key in x.nodes.iter() {
                            let cont=x.repo.contents(key);
                            if cont.len() > 0 && !first { // If this is the first non-empty line of this side of the conflict
                                first=true;
                                // Either we've had another side of the conflict before
                                if ! x.is_first {x.b.conflict_next();}
                                // Or not
                                else {
                                    x.b.begin_conflict();
                                    x.is_first=false
                                }
                            }
                            x.b.output_line(key,cont)
                        }
                        x.next=i
                    } else {
                        // Pour chaque permutation de la SCC, ajouter tous les sommets sur la pile, et appel recursif de chaque arete non-forward.
                        fn permutations<'b,'a:'b,'c,B:LineBuffer<'a>>(
                            params:&mut A<'b,'a,'c,B>,
                            i:usize,
                            j:usize,
                            next:&mut HashSet<usize>) {
                            debug!(target:"conflict","permutations:j={}, nodes={:?}",j,params.nodes);
                            if j<params.scc[i].len() {
                                debug!(target:"conflict","next? j={} {}",j,next.len());
                                let n=params.g.lines[params.scc[i][j]].n_children;
                                debug!(target:"conflict","n={}",n);
                                for c in 0 .. n {
                                    let (edge_child,n_child) = params.g.children[params.g.lines[params.scc[i][j]].children + c];
                                    if n_child != 0 || edge_child.is_null() {
                                        // Not a forward edge (forward edges are (!=NULL, 0)).
                                        debug!(target:"conflict","n_child={}",n_child);
                                        next.insert(params.g.lines[n_child].scc);
                                    }
                                }
                                for k in j..params.scc[i].len() {
                                    params.scc[i].swap(j,k);
                                    let mut newly_forced=Vec::new();
                                    let key=params.g.lines[params.scc[i][j]].key;
                                    let mut key_is_present = true;
                                    if params.g.lines[params.scc[i][j]].is_zombie() {
                                        let mut is_forced:bool = false;
                                        let mut is_defined:bool = false;
                                        for parent in CursIter::new(params.cursor,key,PARENT_EDGE,true,true) {
                                            let f=&parent[(1+KEY_SIZE)..(1+KEY_SIZE+HASH_SIZE)];
                                            match params.selected_zombies.get(f) {
                                                Some(force)=>{
                                                    is_defined = true;
                                                    is_forced = *force
                                                },
                                                None => {
                                                    newly_forced.push(f)
                                                }
                                            }
                                        }
                                        debug!(target:"conflict","forced:{:?}",is_forced);
                                        // If this zombie line is not forced in, try without it.
                                        if !is_defined {
                                            // pas defini, on le definit.
                                            for f in newly_forced.iter() {
                                                params.selected_zombies.insert(f,false);
                                            }
                                        } else {
                                            key_is_present = is_forced
                                        }
                                        if !is_forced {
                                            permutations(params,i,j+1,next)
                                        }
                                        if key_is_present {
                                            for f in newly_forced.iter() {
                                                params.selected_zombies.insert(f,true);
                                            }
                                        }
                                    }
                                    if key_is_present {
                                        params.nodes.push(key);
                                        permutations(params,i,j+1,next);
                                        params.nodes.pop();
                                    }
                                    if newly_forced.len()>0 {
                                        // Unmark here.
                                        for f in &newly_forced {
                                            params.selected_zombies.remove(f);
                                        }
                                    }
                                }
                            } else {
                                debug!(target:"conflict","next? {}",next.len());
                                for chi in next.iter() {
                                    debug!(target:"conflict","rec: get_conflict {}",*chi);
                                    get_conflict(params,*chi);
                                }
                            }
                        }
                        let mut next=HashSet::new();
                        debug!(target:"conflict","permutations");
                        permutations(x,i,0,&mut next);
                    }
                }
                nodes.clear();
                let (next,is_first)={
                    let mut conflict= A {
                        repo:self,
                        scc:&mut scc,
                        first_visit:&first_visit,
                        last_visit:&last_visit,
                        g:&g,
                        b:buf,
                        next:0,
                        nodes:&mut nodes,
                        is_first:true,
                        selected_zombies:&mut selected_zombies,
                        cursor:cursor
                    };
                    get_conflict(&mut conflict,i);
                    (conflict.next,conflict.is_first)
                };
                if !is_first { buf.end_conflict() }
                if i==0 { break } else { i=std::cmp::min(i-1,next) }
            }
        }
        debug!(target:"conflict","/output_file");
    }
    fn remove_redundant_edges(&mut self,forward:&mut Vec<u8>) {
        let mut i=0;
        let cursor=unsafe { self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        while i<forward.len() {
            debug!(target:"remove_redundant_edges","i={},forward.len={}",i,forward.len());
            unsafe {
                let (_,v)=lmdb::cursor_get(cursor,
                                           &forward[(i+1)..(i+1+KEY_SIZE)],
                                           Some(&forward[(i+1+KEY_SIZE+HASH_SIZE)..
                                                         (i+1+KEY_SIZE+HASH_SIZE+1+KEY_SIZE)]),
                                           lmdb::Op::MDB_GET_BOTH_RANGE).unwrap();
                // vérifier que c'est le bon résultat.
                if memcmp(v.as_ptr() as *const c_void,
                          forward.as_ptr().offset((i+1+KEY_SIZE+HASH_SIZE) as isize) as *const c_void,
                          (1+KEY_SIZE) as size_t) == 0 {

                    copy_nonoverlapping(v.as_ptr().offset((1+KEY_SIZE) as isize),
                                        forward.as_mut_ptr().offset((i+1+KEY_SIZE) as isize),
                                        HASH_SIZE);
                }
                lmdb::cursor_del(cursor,0).unwrap();
                self.mdb_txn.del(self.dbi_nodes,
                                 &forward[(i+1+KEY_SIZE+HASH_SIZE+1)..(i+1+KEY_SIZE+HASH_SIZE+1+KEY_SIZE)],
                                 Some(&forward[i..(i+1+KEY_SIZE+HASH_SIZE)])).unwrap();
            }
            i+=(1+HASH_SIZE+KEY_SIZE) + (1+KEY_SIZE)
        }
        unsafe { lmdb::mdb_cursor_close(cursor) };
    }

    /// Gets the external key corresponding to the given key, returning an
    /// owned vector. If the key is just a patch id, it returns the
    /// corresponding external hash.
    fn external_key(&self,key:&[u8])->ExternalKey {
        let mut result= self.external_hash(&key[0..HASH_SIZE]).to_vec();
        if key.len()==KEY_SIZE { result.extend(&key[HASH_SIZE..KEY_SIZE]) };
        result
    }

    fn external_hash(&self,key:&[u8])->&[u8] {
        //println!("internal key:{:?}",&key[0..HASH_SIZE]);
        if key.len()>=HASH_SIZE
            && unsafe {memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)}==0 {
                //println!("is root key");
                &ROOT_KEY[0..HASH_SIZE]
            } else {
                match self.mdb_txn.get(self.dbi_external,&key[0..HASH_SIZE]) {
                    Ok(Some(pv))=> {
                        pv
                    },
                    Ok(None)=>{
                        println!("internal key or hash:{:?}",key);
                        panic!("external hash not found !")
                    },
                    Err(_)=>{
                        println!("internal key:{:?}",key);
                        panic!("LMDB error !")
                    }
                }
            }
    }


    pub fn internal_hash<'b>(&'b self,key:&[u8])->Result<&'b [u8],Error> {
        debug!("internal_hash: {}, {}",to_hex(key), key.len());
        if key.len()==HASH_SIZE
            && unsafe { memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t) }==0 {
                Ok(ROOT_KEY)
            } else {
                match try!(self.mdb_txn.get(self.dbi_internal,key)) {
                    Some(k)=>Ok(k),
                    None=>Err(Error::InternalHashNotFound(key.to_vec()))
                }
            }
    }
    /*
    fn internal_key(&'a self,key:&[u8],internal_patch_id:&[u8],result:&mut [u8])->Result<(),Error> {
        debug_assert!(result.len()>=KEY_SIZE);
        let inter=
            if key.len() == LINE_SIZE { internal_patch_id } else {
                try!(self.internal_hash(&key[0..(key.len()-LINE_SIZE)]))
            };
        unsafe {
            copy_nonoverlapping(inter.as_ptr(),result.as_mut_ptr(),HASH_SIZE);
            copy_nonoverlapping(key.as_ptr().offset((key.len()-LINE_SIZE) as isize),
                                result.as_mut_ptr().offset(HASH_SIZE as isize),
                                LINE_SIZE)
        }
        Ok(())
    }
     */
    /// "intro" is the internal patch number of the patch that introduced this edge.
    fn internal_edge(&'a self,flag:u8,to:&[u8],intro:&[u8],result:&mut [u8])->Result<(),Error> {
        debug_assert!(result.len()>=1+KEY_SIZE+HASH_SIZE);
        debug_assert!(intro.len() == HASH_SIZE);
        result[0]=flag;
        let int_to=try!(self.internal_hash(&to[0..(to.len()-LINE_SIZE)]));
        unsafe {
            copy_nonoverlapping(int_to.as_ptr(),result.as_mut_ptr().offset(1),HASH_SIZE);
            copy_nonoverlapping(to.as_ptr().offset((to.len()-LINE_SIZE) as isize),
                                result.as_mut_ptr().offset((1+HASH_SIZE) as isize),
                                LINE_SIZE);
            copy_nonoverlapping(intro.as_ptr(),result.as_mut_ptr().offset(1+KEY_SIZE as isize),HASH_SIZE);
        }
        Ok(())
    }
    /// Create a new internal patch id, register it in the "external" and
    /// "internal" bases, and write the result in its second argument
    /// ("result").
    ///
    /// When compiled in debug mode, this function is deterministic
    /// and returns the last registered patch number, plus one (in big
    /// endian binary on HASH_SIZE bytes). Otherwise, it returns a
    /// random patch number not yet registered.
    pub fn new_internal(&mut self,result:&mut[u8]) {

        if cfg!(debug_assertions){
            let curs=self.mdb_txn.cursor(self.dbi_external).unwrap();
            if let Ok((k,_))=curs.get(b"",None,lmdb::Op::MDB_LAST) {
                unsafe { copy_nonoverlapping(k.as_ptr() as *const c_void,result.as_mut_ptr() as *mut c_void, HASH_SIZE) }
            } else {
                for i in 0..HASH_SIZE { result[i]=0 }
            };
            let mut i=HASH_SIZE-1;
            while i>0 && result[i]==0xff {
                result[i]=0;
                i-=1
            }
            if result[i] != 0xff {
                result[i]+=1
            } else {
                panic!("the last patch in the universe has arrived")
            }
        } else {
            for i in 0..result.len() { result[i]=rand::random() }
            loop {
                match self.mdb_txn.get(self.dbi_external,&result) {
                    Ok(None)=>break,
                    Ok(_)=>{for i in 0..result.len() { result[i]=rand::random() }},
                    Err(_)=>panic!("")
                }
            }
        }
    }

    pub fn register_hash(&mut self,internal:&[u8],external:&[u8]){
        debug!(target:"apply","registering patch\n  {}\n  as\n  {}",
               to_hex(external),to_hex(internal));
        self.mdb_txn.put(self.dbi_external,internal,external,0).unwrap();
        self.mdb_txn.put(self.dbi_internal,external,internal,0).unwrap();
    }


    fn delete_edges(&self, cursor:&mut lmdb::MdbCursor,edges:&mut Vec<Edge>, key:&'a[u8],flag:u8){
        if key.len() > 0 {
            let ext_key=self.external_key(key);
            for v in CursIter::new(cursor,key,flag,true,false) {
                edges.push(Edge { from:ext_key.clone(),
                                  to:self.external_key(&v[1..(1+KEY_SIZE)]),
                                  introduced_by:self.external_key(&v[(1+KEY_SIZE)..]) });
            }
        }
    }

    fn diff(&self,line_num:&mut usize, actions:&mut Vec<Change>, redundant:&mut Vec<u8>,
            a:Graph, b:&Path)->Result<(),std::io::Error> {
        fn memeq(a:&[u8],b:&[u8])->bool {
            if a.len() == b.len() {
                unsafe { memcmp(a.as_ptr() as *const c_void,b.as_ptr() as *const c_void,
                                b.len() as size_t) == 0 }
            } else { false }
        }
        fn local_diff(repo:&Repository,cursor:&mut lmdb::MdbCursor,actions:&mut Vec<Change>,line_num:&mut usize, lines_a:&[&[u8]], contents_a:&[&[u8]], b:&[&[u8]]) {
            debug!(target:"conflictdiff","local_diff {} {}",contents_a.len(),b.len());
            let mut opt=vec![vec![0;b.len()+1];contents_a.len()+1];
            if contents_a.len()>0 {
                let mut i=contents_a.len() - 1;
                loop {
                    opt[i]=vec![0;b.len()+1];
                    if b.len()>0 {
                        let mut j=b.len()-1;
                        loop {
                            opt[i][j]=
                                if memeq(contents_a[i],b[j]) {
                                    opt[i+1][j+1]+1
                                } else {
                                    std::cmp::max(opt[i+1][j], opt[i][j+1])
                                };
                            debug!(target:"diff","opt[{}][{}] = {}",i,j,opt[i][j]);
                            if j>0 { j-=1 } else { break }
                        }
                    }
                    if i>0 { i-=1 } else { break }
                }
            }
            let mut i=1;
            let mut j=0;
            fn add_lines(repo:&Repository,actions:&mut Vec<Change>, line_num:&mut usize,
                         up_context:&[u8],down_context:&[&[u8]],lines:&[&[u8]]){
                actions.push(
                    Change::NewNodes {
                        up_context:vec!(repo.external_key(up_context)),
                        down_context:down_context.iter().map(|x|{repo.external_key(x)}).collect(),
                        line_num: *line_num as u32,
                        flag:0,
                        nodes:lines.iter().map(|x|{x.to_vec()}).collect()
                    });
                *line_num += lines.len()
            }
            fn delete_lines(repo:&Repository,cursor:&mut lmdb::MdbCursor,actions:&mut Vec<Change>, lines:&[&[u8]]){
                let mut edges=Vec::with_capacity(lines.len());
                for i in 0..lines.len() {
                    //unsafe {println!("- {}",std::str::from_utf8_unchecked(repo.contents(lines[i])));}
                    debug!(target:"conflictdiff","deleting line {}",to_hex(lines[i]));
                    repo.delete_edges(cursor,&mut edges,lines[i],PARENT_EDGE)
                }
                actions.push(Change::Edges{edges:edges,flag:PARENT_EDGE|DELETED_EDGE})
            }
            let mut oi=None;
            let mut oj=None;
            let mut last_alive_context=0;
            while i<contents_a.len() && j<b.len() {
                debug!(target:"diff","i={}, j={}",i,j);
                if memeq(contents_a[i],b[j]) {
                    if let Some(i0)=oi {
                        debug!(target:"diff","deleting from {} to {} / {}",i0,i,lines_a.len());
                        delete_lines(repo,cursor,actions, &lines_a[i0..i]);
                        oi=None
                    } else if let Some(j0)=oj {
                        add_lines(repo,actions, line_num,
                                  lines_a[last_alive_context], // up context
                                  &lines_a[i..i+1], // down context
                                  &b[j0..j]);
                        oj=None
                    }
                    last_alive_context=i;
                    i+=1; j+=1;
                } else {
                    if opt[i+1][j] >= opt[i][j+1] {
                        // we will delete things starting from i (included).
                        if let Some(j0)=oj {
                            add_lines(repo,actions, line_num,
                                      lines_a[last_alive_context], // up context
                                      &lines_a[i..i+1], // down context
                                      &b[j0..j]);
                            oj=None
                        }
                        if oi.is_none() {
                            oi=Some(i)
                        }
                        i+=1
                    } else {
                        // We will add things starting from j.
                        if let Some(i0)=oi {
                            delete_lines(repo,cursor,actions, &lines_a[i0..i]);
                            last_alive_context=i0-1;
                            oi=None
                        }
                        if oj.is_none() {
                            oj=Some(j)
                        }
                        j+=1
                    }
                }
            }
            if i < lines_a.len() {
                if let Some(j0)=oj {
                    add_lines(repo,actions, line_num,
                              lines_a[i-1], // up context
                              &lines_a[i..i+1], // down context
                              &b[j0..j])
                }
                delete_lines(repo,cursor,actions,&lines_a[i..lines_a.len()])
            } else if j < b.len() {
                if let Some(i0)=oi {
                    delete_lines(repo,cursor,actions, &lines_a[i0..i]);
                    add_lines(repo,actions, line_num, lines_a[i0-1], &[], &b[j..b.len()])
                } else {
                    add_lines(repo,actions, line_num, lines_a[i-1], &[], &b[j..b.len()])
                }
            }
        }

        let mut buf_b=Vec::new();
        let mut lines_b=Vec::new();
        let err={
            let err={
                let f = std::fs::File::open(b);
                let mut f = std::io::BufReader::new(f.unwrap());
                f.read_to_end(&mut buf_b)
            };
            let mut i=0;
            let mut j=0;

            while j<buf_b.len() {
                if buf_b[j]==0xa {
                    lines_b.push(&buf_b[i..j+1]);
                    i=j+1
                }
                j+=1;
            }
            if i<j { lines_b.push(&buf_b[i..j]) }
            err
        };
        match err {
            Ok(_)=>{
                let t0=time::precise_time_s();
                let mut d = Diff { lines_a:Vec::new(), contents_a:Vec::new() };
                self.output_file(&mut d,a,redundant);
                let t1=time::precise_time_s();
                info!("output_file took {}s",t1-t0);
                let cursor= unsafe {&mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()};
                local_diff(self,cursor,actions, line_num,
                           &d.lines_a,
                           &d.contents_a,
                           &lines_b);
                unsafe {
                    lmdb::mdb_cursor_close(cursor);
                }
                let t2=time::precise_time_s();
                info!("diff took {}s",t2-t1);
                Ok(())
            },
            Err(e)=>Err(e)
        }
    }



    fn record_all(&self,
                  actions:&mut Vec<Change>,
                  line_num:&mut usize,
                  redundant:&mut Vec<u8>,
                  updatables:&mut HashMap<Vec<u8>,Vec<u8>>,
                  parent_inode:Option<&[u8]>,
                  parent_node:Option<&[u8]>,
                  current_inode:&[u8],
                  realpath:&mut std::path::PathBuf,
                  basename:&[u8])->Result<(),Error> {

        if parent_inode.is_some() { realpath.push(str::from_utf8(&basename).unwrap()) }
        debug!(target:"record_all","realpath:{:?}",realpath);

        let mut l2=[0;LINE_SIZE];
        let current_node=
            if parent_inode.is_some() {
                match self.mdb_txn.get(self.dbi_inodes,&current_inode) {
                    Ok(Some(current_node))=>{
                        let old_attr=((current_node[1] as usize) << 8) | (current_node[2] as usize);
                        // Add the new name.
                        let int_attr={
                            let attr=try!(metadata(&realpath));
                            let p=(permissions(&attr)) & 0o777;
                            let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                            (if p==0 { old_attr } else { p }) | is_dir
                        };
                        debug!(target:"record_all","current_node[0]={},old_attr={},int_attr={}",
                               current_node[0],old_attr,int_attr);
                        if current_node[0]==1 || old_attr!=int_attr {
                            // file moved

                            // Delete all former names.
                            let mut edges=Vec::new();
                            // Now take all grandparents of l2, delete them.
                            let mut curs_parents=unsafe {
                                &mut *try!(self.mdb_txn.unsafe_cursor(self.dbi_nodes))
                            };
                            let mut curs_grandparents=unsafe {
                                &mut *try!(self.mdb_txn.unsafe_cursor(self.dbi_nodes))
                            };
                            let mut name=Vec::with_capacity(basename.len()+2);
                            name.push(((int_attr >> 8) & 0xff) as u8);
                            name.push((int_attr & 0xff) as u8);
                            name.extend(basename);
                            for parent in CursIter::new(curs_parents,&current_node[3..],FOLDER_EDGE|PARENT_EDGE,true,false) {
                                let previous_name=
                                    match try!(self.mdb_txn.get(self.dbi_contents,&parent[1..(1+KEY_SIZE)])) {
                                        None=>"".as_bytes(),
                                        Some(n)=>n
                                    };
                                for grandparent in CursIter::new(curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE|PARENT_EDGE,true,false) {
                                    if &grandparent[1..(1+KEY_SIZE)] != parent_node.unwrap()
                                        || &name[..] != previous_name {
                                            edges.push(Edge {
                                                from:self.external_key(&parent[1..(1+KEY_SIZE)]),
                                                to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                                introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                            });
                                        }
                                }
                            }
                            unsafe {
                                lmdb::mdb_cursor_close(curs_parents);
                                lmdb::mdb_cursor_close(curs_grandparents);
                            }
                            debug!(target:"record_all", "edges:{:?}",edges);
                            if !edges.is_empty(){
                                actions.push(Change::Edges{edges:edges,flag:DELETED_EDGE|FOLDER_EDGE|PARENT_EDGE});
                                //debug!(target:"record_all","parent_node: {:?}",parent_node.unwrap());
                                //debug!(target:"record_all","ext key: {:?}",self.external_key(parent_node.unwrap()));
                                //debug!(target:"record_all","ext key: {:?}",self.external_key(&current_node[3..]));
                                actions.push(
                                    Change::NewNodes { up_context:{
                                        let p=parent_node.unwrap();
                                        vec!(if p.len()>LINE_SIZE { self.external_key(&p) }
                                             else { p.to_vec() })
                                    },
                                                       line_num: *line_num as u32,
                                                       down_context:{
                                                           let p=&current_node[3..];
                                                           vec!(if p.len()>LINE_SIZE { self.external_key(&p) }
                                                                else { p.to_vec() })
                                                       },
                                                       nodes: vec!(name),
                                                       flag:FOLDER_EDGE }
                                    );
                            }
                            *line_num += 1;
                            debug!(target:"record_all", "directory_flag:{}",old_attr&DIRECTORY_FLAG);
                            if old_attr & DIRECTORY_FLAG == 0 {
                                info!("retrieving");
                                let time0=time::precise_time_s();
                                let ret=self.retrieve(&current_node[3..]);
                                let time1=time::precise_time_s();
                                info!("retrieve took {}s, now calling diff", time1-time0);
                                self.diff(line_num,actions, redundant,ret.unwrap(), realpath.as_path()).unwrap();
                                let time2=time::precise_time_s();
                                info!("total diff took {}s", time2-time1);
                            }

                        } else if current_node[0]==2 {
                            let mut edges=Vec::new();
                            // Now take all grandparents of l2, delete them.
                            let mut curs_parents= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
                            let mut curs_grandparents= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
                            for parent in CursIter::new(curs_parents,&current_node[3..],FOLDER_EDGE|PARENT_EDGE,true,false) {
                                for grandparent in CursIter::new(curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE|PARENT_EDGE,true,false) {
                                    edges.push(Edge {
                                        from:self.external_key(&parent[1..(1+KEY_SIZE)]),
                                        to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                        introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                    });
                                }
                            }

                            // file deleted. delete recursively
                            let mut file_edges=vec!();
                            {
                                debug!(target:"record_all","del={}",to_hex(current_node));
                                let ret = self.retrieve(&current_node[3..]).unwrap();
                                for l in ret.lines {
                                    if l.key.len()>0 {
                                        let ext_key=self.external_key(l.key);
                                        debug!(target:"record_all","ext_key={}",to_hex(&ext_key));
                                        for v in CursIter::new(curs_parents,l.key,PARENT_EDGE,true,false) {
                                            debug!(target:"record_all","v={}",to_hex(v));
                                            if v[0] & FOLDER_EDGE != 0 { &mut edges } else { &mut file_edges }
                                            .push(Edge { from:ext_key.clone(),
                                                         to:self.external_key(&v[1..(1+KEY_SIZE)]),
                                                         introduced_by:self.external_key(&v[(1+KEY_SIZE)..]) });
                                        }
                                    }
                                }
                            }
                            unsafe {
                                lmdb::mdb_cursor_close(curs_parents);
                                lmdb::mdb_cursor_close(curs_grandparents);
                            }

                            actions.push(Change::Edges{edges:edges,flag:FOLDER_EDGE|PARENT_EDGE|DELETED_EDGE});
                            if file_edges.len()>0 {
                                actions.push(Change::Edges{edges:file_edges,flag:PARENT_EDGE|DELETED_EDGE});
                            }
                        } else if current_node[0]==0 {
                            if old_attr & DIRECTORY_FLAG == 0 {
                                let time0=time::precise_time_s();
                                let ret=self.retrieve(&current_node[3..]);
                                let time1=time::precise_time_s();
                                info!(target:"record_all","record: retrieve took {}s, now calling diff", time1-time0);
                                self.diff(line_num,actions, redundant,ret.unwrap(), realpath.as_path()).unwrap();
                                let time2=time::precise_time_s();
                                info!(target:"record_all","total diff took {}s", time2-time1);
                            }
                        } else {
                            panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                        };
                        Some(&current_node[3..])
                    },
                    Ok(None)=>{
                        // File addition, create appropriate Newnodes.
                        debug!(target:"record_all","metadata");
                        match metadata(&realpath) {
                            Ok(attr) => {
                                let int_attr={
                                    let attr=metadata(&realpath).unwrap();
                                    let p=permissions(&attr);
                                    let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                                    (if p==0 { 0o755 } else { p }) | is_dir
                                };
                                let mut nodes=Vec::new();
                                let mut lnum= *line_num + 1;
                                for i in 0..(LINE_SIZE-1) { l2[i]=(lnum & 0xff) as u8; lnum=lnum>>8 }

                                let mut name=Vec::with_capacity(basename.len()+2);
                                name.push(((int_attr >> 8) & 0xff) as u8);
                                name.push((int_attr & 0xff) as u8);
                                name.extend(basename);
                                actions.push(
                                    Change::NewNodes { up_context: vec!(
                                        if parent_node.unwrap().len()>LINE_SIZE { self.external_key(parent_node.unwrap()) }
                                        else {parent_node.unwrap().to_vec()}
                                            ),
                                                       line_num: *line_num as u32,
                                                       down_context: vec!(),
                                                       nodes: vec!(name,vec!()),
                                                       flag:FOLDER_EDGE }
                                    );
                                *line_num += 2;
                                updatables.insert(l2.to_vec(),current_inode.to_vec());
                                // Reading the file
                                if !attr.is_dir() {
                                    nodes.clear();
                                    let mut line=Vec::new();
                                    let f = std::fs::File::open(realpath.as_path());
                                    let mut f = std::io::BufReader::new(f.unwrap());
                                    loop {
                                        match f.read_until('\n' as u8,&mut line) {
                                            Ok(l) => if l>0 { nodes.push(line.clone());line.clear() } else { break },
                                            Err(_) => break
                                        }
                                    }
                                    let len=nodes.len();
                                    if !nodes.is_empty() {
                                        actions.push(
                                            Change::NewNodes { up_context:vec!(l2.to_vec()),
                                                               line_num: *line_num as u32,
                                                               down_context: vec!(),
                                                               nodes: nodes,
                                                               flag:0 }
                                            );
                                    }
                                    *line_num+=len;
                                    None
                                } else {
                                    Some(&l2[..])
                                }
                            },
                            Err(_)=>{
                                panic!("error adding a file (metadata failed)")
                            }
                        }
                    },
                    Err(_)=>{
                        panic!("lmdb error")
                    }
                }
            } else {
                Some(ROOT_KEY)
            };
        debug!(target:"record_all","current_node={:?}",current_node);
        match current_node {
            None => (), // we just added a file
            Some(current_node)=>{
                debug!(target:"record_all","children of current_inode {}",to_hex(current_inode));
                let cursor=try!(self.mdb_txn.cursor(self.dbi_tree));
                let mut op=lmdb::Op::MDB_SET_RANGE;
                while let Ok((k,v))=cursor.get(current_inode,None,op) {
                    if unsafe{memcmp(k.as_ptr() as *const c_void,
                                     current_inode.as_ptr() as *const c_void,
                                     INODE_SIZE as size_t) } != 0 {
                        break
                    } else {
                        if v.len()>0 { // directories have len==0
                            debug!(target:"record_all","  child: {} + {}",to_hex(&v[0..INODE_SIZE]),
                                   std::str::from_utf8(&k[INODE_SIZE..]).unwrap());
                            try!(self.record_all(actions, line_num,redundant,updatables,
                                                 Some(current_inode), // parent_inode
                                                 Some(current_node), // parent_node
                                                 v,// current_inode
                                                 realpath,
                                                 &k[INODE_SIZE..]));
                        }
                        op=lmdb::Op::MDB_NEXT;
                    }
                }
            }
        }
        if parent_inode.is_some() { let _=realpath.pop(); }
        Ok(())
    }

    /// Records,i.e. produce a patch and a HashMap mapping line numbers to inodes.
    pub fn record(&mut self,working_copy:&std::path::Path)->Result<(Vec<Change>,HashMap<LocalKey,Inode>),Error>{
        let mut actions:Vec<Change>=Vec::new();
        let mut line_num=1;
        let mut updatables:HashMap<Vec<u8>,Vec<u8>>=HashMap::new();
        let mut realpath=PathBuf::from(working_copy);
        let mut redundant=vec!();
        try!(self.record_all(&mut actions, &mut line_num,&mut redundant,&mut updatables,
                             None,None,ROOT_INODE,&mut realpath,
                             &[]));
        debug!(target:"record","record done, {} changes", actions.len());
        self.remove_redundant_edges(&mut redundant);
        debug!("remove_redundant_edges done");
        Ok((actions,updatables))
    }

    /// Test whether a node has edges unknown to the patch we're applying.
    fn has_exclusive_edge(&self,cursor:&mut lmdb::MdbCursor,internal_patch_id:&[u8],key:&[u8],flag0:u8,include_folder:bool,include_pseudo:bool,dependencies:&HashSet<Vec<u8>>)->bool {
        for neighbor in CursIter::new(cursor,&key[1..(1+KEY_SIZE)],flag0,include_folder,include_pseudo) {
            if unsafe {
                memcmp(neighbor.as_ptr().offset(1+KEY_SIZE as isize) as *const c_void,
                       internal_patch_id.as_ptr() as *const c_void,
                       HASH_SIZE as size_t)
                    != 0 } {
                debug!(target:"exclusive","exclusive_edge: {}",to_hex(&neighbor));
                let ext=self.external_hash(&neighbor[(1+KEY_SIZE)..(1+KEY_SIZE+HASH_SIZE)]);
                debug!(target:"exclusive", "{:?}, neighbor={},\next={}",flag0,to_hex(&neighbor[(1+KEY_SIZE)..(1+KEY_SIZE+HASH_SIZE)]),to_hex(ext));
                if !dependencies.contains(ext) {
                    return true
                } else {
                    for p in dependencies.iter() {
                        debug!(target:"exclusive","p={}",to_hex(p));
                    }
                }
            }
        }
        false
    }


    fn unsafe_apply(&mut self,changes:&[Change], internal_patch_id:&[u8],dependencies:&HashSet<Vec<u8>>)->Result<(),Error>{
        debug!(target:"conflictdiff","unsafe_apply");
        let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        let alive= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let cursor= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let mut parents:Vec<u8>=Vec::new();
        let mut children:Vec<u8>=Vec::new();
        debug!(target:"apply","unsafe_apply (patch {})",to_hex(internal_patch_id));
        for ch in changes {
            match *ch {
                Change::Edges{ref flag, ref edges} => {
                    // If this hunk deletes nodes that are not known to the author of the current patch, add pseudo-edges (zombie lines) to each edge of this hunk.
                    let mut add_zombies=false;
                    for e in edges {
                        // First remove the deleted version of the edge
                        //debug!(target:"conflictdiff","e:{:?}",e);
                        {
                            let p= try!(self.internal_hash(&e.introduced_by));
                            try!(self.internal_edge(*flag^DELETED_EDGE^PARENT_EDGE,&e.from,p,&mut pu));
                            try!(self.internal_edge(*flag^DELETED_EDGE,&e.to,p,&mut pv));
                            debug!(target:"exclusive","pu={}\npv={}",to_hex(&pu),to_hex(&pv));
                        }
                        try!(self.mdb_txn.del(self.dbi_nodes,&pu[1..(1+KEY_SIZE)], Some(&pv)));
                        try!(self.mdb_txn.del(self.dbi_nodes,&pv[1..(1+KEY_SIZE)], Some(&pu)));

                        if *flag & DELETED_EDGE != 0 {
                            // Will we need zombies?
                            // We need internal_patch_id here: previous hunks of this patch could have added edges to us.
                            if self.has_exclusive_edge(cursor,internal_patch_id,&pv,PARENT_EDGE,true,true,dependencies)
                                || self.has_exclusive_edge(cursor,internal_patch_id,&pu,0,true,true,dependencies) {
                                    debug!(target:"exclusive","add_zombies:\n{}\n{}",
                                           to_hex(&e.to),to_hex(&e.from));
                                    add_zombies=true;
                                } else {
                                    debug!(target:"exclusive","not add zombies: {}",add_zombies);
                                }
                            //
                            self.kill_obsolete_pseudo_edges(cursor, if *flag&PARENT_EDGE == 0 { &mut pv } else { &mut pu })
                        }
                    }
                    // Then add the new edges.
                    // Then add zombies and pseudo-edges if needed.
                    debug!(target:"apply","edges (patch {})",to_hex(internal_patch_id));
                    parents.clear();
                    children.clear();
                    for e in edges {
                        try!(self.internal_edge(*flag^PARENT_EDGE,&e.from,internal_patch_id,&mut pu));
                        try!(self.internal_edge(*flag,&e.to,internal_patch_id,&mut pv));
                        debug!(target:"apply","new edge:\n  {}\n  {}",to_hex(&pu),to_hex(&pv));
                        try!(self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA));
                        try!(self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA));
                        // Here, there are two options: either we need zombie lines because the currently applied patch doesn't know about some of our edges, or else we just need to reconnect parents and children of a deleted portion of the graph.
                        if *flag & DELETED_EDGE != 0 {
                            if add_zombies {
                                pu[0]^= DELETED_EDGE;
                                pv[0]^= DELETED_EDGE;
                                debug!(target:"apply","zombie:\n  {}\n  {}",to_hex(&pu),to_hex(&pv));
                                try!(self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA));
                                try!(self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA));
                            } else if *flag & FOLDER_EDGE == 0 {
                                // collect alive parents/children of hunk
                                let (pu,pv)= if *flag&PARENT_EDGE==0 { (&pu,&pv) } else { (&pv,&pu) };
                                debug!(target:"apply","pu has_edge\n  {}",to_hex(&pu[1..(1+KEY_SIZE)]));
                                if has_edge(cursor,&pu[1..(1+KEY_SIZE)],PARENT_EDGE,true,true) {
                                    let i=parents.len();
                                    parents.extend(&pu[..]);
                                    parents[i]^= PSEUDO_EDGE | DELETED_EDGE;
                                }
                                debug!(target:"apply","pv={}",to_hex(&pv[1..(1+KEY_SIZE)]));
                                for neighbor in CursIter::new(cursor,&pv[1..(1+KEY_SIZE)],PARENT_EDGE,true,true) {
                                    debug!(target:"apply","parent has_edge\n  {}",to_hex(neighbor));
                                    if has_edge(alive,&neighbor[1..(1+KEY_SIZE)],PARENT_EDGE,true,true) {
                                        let i=parents.len();
                                        parents.extend(neighbor);
                                        parents[i]^=PSEUDO_EDGE;
                                    }
                                }
                                for neighbor in CursIter::new(cursor,&pv[1..(1+KEY_SIZE)],0,true,true) {
                                    debug!(target:"apply","children has_edge\n  {}",to_hex(neighbor));
                                    if has_edge(alive,&neighbor[1..(1+KEY_SIZE)],PARENT_EDGE,true,true) {
                                        let i=children.len();
                                        children.extend(neighbor);
                                        children[i]^=PSEUDO_EDGE;
                                    }
                                }
                            }
                        }
                    }
                    debug!(target:"apply","/edges");
                    // Finally: reconnect
                    if *flag &DELETED_EDGE != 0 {
                        let mut i=0;
                        while i<children.len() {
                            let mut j=0;
                            while j<parents.len() {
                                if ! self.connected(cursor,
                                                    &parents[j+1 .. j+1+KEY_SIZE],
                                                    &mut children[i .. i+1+KEY_SIZE+HASH_SIZE]) {
                                    debug!(target:"apply","reconnect:\n  {}\n  {}",
                                           to_hex(&parents[j..(j+1+KEY_SIZE+HASH_SIZE)]),
                                           to_hex(&mut children[i..(i+1+KEY_SIZE+HASH_SIZE)]));
                                    if unsafe {
                                        memcmp(parents.as_ptr().offset((j+1) as isize) as *const c_void,
                                               children.as_ptr().offset((i+1) as isize) as *const c_void,
                                               KEY_SIZE as size_t) != 0
                                    } {
                                        self.add_edge(&parents[j..(j+1+KEY_SIZE+HASH_SIZE)],
                                                      &mut children[i..(i+1+KEY_SIZE+HASH_SIZE)]);
                                    }
                                }
                                j+=1+KEY_SIZE+HASH_SIZE;
                            }
                            i+=1+KEY_SIZE+HASH_SIZE;
                        }
                    }
                    debug!(target:"libpijul","unsafe_apply:edges.done");
                },
                Change::NewNodes { ref up_context,ref down_context,ref line_num,ref flag,ref nodes } => {
                    assert!(!nodes.is_empty());
                    debug!(target:"libpijul","unsafe_apply: newnodes");
                    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut lnum0= *line_num;
                    for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    unsafe {
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pv.as_mut_ptr().offset(1),
                                            HASH_SIZE);
                    };
                    for c in up_context {
                        {
                            debug!("newnodes: up_context {:?}",to_hex(&c));

                            let u= if c.len()>LINE_SIZE {
                                try!(self.internal_hash(&c[0..(c.len()-LINE_SIZE)]))
                            } else {
                                internal_patch_id
                            };
                            pu[0]= (*flag) ^ PARENT_EDGE;
                            pv[0]= *flag;
                            unsafe {
                                copy_nonoverlapping(u.as_ptr() as *const c_char,
                                                    pu.as_mut_ptr().offset(1) as *mut c_char,
                                                    HASH_SIZE);
                                copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                                    pu.as_mut_ptr().offset((1+HASH_SIZE) as isize),
                                                    LINE_SIZE);
                            }
                        }
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                    }
                    unsafe {
                        copy_nonoverlapping(internal_patch_id.as_ptr() as *const c_char,
                                            pu.as_ptr().offset(1) as *mut c_char,
                                            HASH_SIZE);
                    }
                    debug!("newnodes: inserting");
                    let mut lnum= *line_num + 1;
                    self.mdb_txn.put(self.dbi_contents,&pv[1..(1+KEY_SIZE)], &nodes[0],0).unwrap();
                    for n in &nodes[1..] {
                        let mut lnum0=lnum-1;
                        for i in 0..LINE_SIZE { pu[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                        lnum0=lnum;
                        for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                        pu[0]= (*flag)^PARENT_EDGE;
                        pv[0]= *flag;
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_contents,&pv[1..(1+KEY_SIZE)],&n,0).unwrap();
                        lnum = lnum+1;
                    }
                    // In this last part, u is that target (downcontext), and v is the last new node.
                    pu[0]= *flag;
                    pv[0]= (*flag) ^ PARENT_EDGE;
                    for c in down_context {
                        {
                            unsafe {
                                let u=if c.len()>LINE_SIZE {
                                    try!(self.internal_hash(&c[0..(c.len()-LINE_SIZE)]))
                                } else {
                                    internal_patch_id
                                };
                                copy_nonoverlapping(u.as_ptr(), pu.as_mut_ptr().offset(1), HASH_SIZE);
                                copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize) as *const c_char,
                                                    pu.as_ptr().offset((1+HASH_SIZE) as isize) as *mut c_char,
                                                    LINE_SIZE);
                            }
                        }
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                        // There was something here before, to remove existing edges between up and down context, but it would break unrecord.
                    }
                }
            }
        }
        unsafe {
            lmdb::mdb_cursor_close(alive);
            lmdb::mdb_cursor_close(cursor)
        };
        Ok(())
    }

    pub fn has_patch(&self, branch:&[u8], hash:&[u8])->Result<bool,Error>{
        if hash.len()==HASH_SIZE && unsafe {memcmp(hash.as_ptr() as *const c_void,
                                                   ROOT_KEY.as_ptr() as *const c_void,
                                                   hash.len() as size_t)==0 } {
            Ok(true)
        } else {
            match self.internal_hash(hash) {
                Ok(internal)=>{
                    let curs=try!(self.mdb_txn.cursor(self.dbi_branches).map_err(Error::IoError));
                    match curs.get(branch,Some(internal),lmdb::Op::MDB_GET_BOTH) {
                        Ok(_)=>Ok(true),
                        Err(_)=>Ok(false)
                    }
                },
                Err(Error::InternalHashNotFound(_))=>Ok(false),
                Err(e)=>Err(e)
            }
        }
    }
    // requires pu to be KEY_SIZE, pv to be 1+KEY_SIZE+HASH_SIZE
    fn connected(&mut self,cursor:*mut lmdb::MdbCursor,pu:&[u8],pv:&mut [u8])->bool{
        let pv_0=pv[0];
        pv[0]=0;
        let result=unsafe { lmdb::cursor_get(cursor,&pu,Some(pv),lmdb::Op::MDB_GET_BOTH_RANGE) };
        pv[0]=pv_0;
        match result {
            Ok((_,v))=>
                unsafe {memcmp(pv.as_ptr() as *const c_void,
                               v.as_ptr() as *const c_void,
                               (1+KEY_SIZE) as size_t) == 0},
            _=>{
                pv[0]=PSEUDO_EDGE;
                let result=unsafe { lmdb::cursor_get(cursor,&pu,Some(pv),lmdb::Op::MDB_GET_BOTH_RANGE) };
                pv[0]=pv_0;
                match result {
                    Ok((_,v))=>
                        unsafe {memcmp(pv.as_ptr() as *const c_void,
                                       v.as_ptr() as *const c_void,
                                       (1+KEY_SIZE) as size_t) == 0 },
                    _=>false
                }
            }
        }
    }
    fn add_edge(&mut self,pu:&[u8],pv:&mut [u8]){
        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
    }

    fn kill_obsolete_pseudo_edges(&mut self,cursor:*mut lmdb::MdbCursor,pv:&[u8]) {
        debug_assert!(pv.len()==1+KEY_SIZE+HASH_SIZE);
        let mut a:[u8;KEY_SIZE]=[0;KEY_SIZE];
        let mut b:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        unsafe {
            copy_nonoverlapping(pv.as_ptr().offset(1) as *const c_void,
                                b.as_mut_ptr().offset(1) as *mut c_void,
                                KEY_SIZE);
        }
        for flag in [PSEUDO_EDGE,PARENT_EDGE|PSEUDO_EDGE,
                     FOLDER_EDGE|PSEUDO_EDGE,PARENT_EDGE|PSEUDO_EDGE|FOLDER_EDGE].iter() {
            let flag=[*flag];
            loop {
                match unsafe { lmdb::cursor_get(cursor,&pv[1..(1+KEY_SIZE)],Some(&flag[..]),lmdb::Op::MDB_GET_BOTH_RANGE) } {
                    Ok((_,v))=>{
                        if v[0]==flag[0] {
                            debug!(target:"kill_obsolete_pseudo","kill_obsolete_pseudo:\n  {}",to_hex(v));
                            unsafe {
                                copy_nonoverlapping(v.as_ptr().offset(1) as *const c_void,
                                                    a.as_mut_ptr() as *mut c_void,
                                                    KEY_SIZE);
                                copy_nonoverlapping(v.as_ptr().offset(1+KEY_SIZE as isize) as *const c_void,
                                                    b.as_mut_ptr().offset(1+KEY_SIZE as isize) as *mut c_void,
                                                    HASH_SIZE);
                            }
                            b[0]=v[0]^PARENT_EDGE;
                            unsafe { lmdb::mdb_cursor_del(cursor,0) };
                            debug!(target:"kill_obsolete_pseudo","kill_obsolete_pseudo (parent):\n  {}\n  {}",to_hex(&a[..]),to_hex(&b[..]));
                            self.mdb_txn.del(self.dbi_nodes,&a[..],Some(&b[..])).unwrap();
                        } else {
                            //debug!(target:"kill_obsolete_pseudo","not kill_obsolete_pseudo:\n  {}",to_hex(v));
                            break
                        }
                    },
                    Err(_)=>break
                }
            }
        }
    }

    /// Applies a patch to a repository. "new_patches" are patches that just this repository has, and the remote repository doesn't have.
    pub fn apply<'b>(&mut self, patch:&Patch, internal:&'b [u8], new_patches:&HashSet<&[u8]>)->Result<(),Error> {
        let current=self.get_current_branch().to_vec();
        {
            let curs=self.mdb_txn.cursor(self.dbi_branches).unwrap();
            match curs.get(&current,Some(internal),lmdb::Op::MDB_GET_BOTH) {
                Ok(_)=>return Err(Error::AlreadyApplied),
                Err(_)=>{}
            }
        }
        self.mdb_txn.put(self.dbi_branches,&current,&internal,lmdb::MDB_NODUPDATA).unwrap();
        try!(self.unsafe_apply(&patch.changes,internal,&patch.dependencies));
        let cursor= unsafe {&mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let cursor_= unsafe {&mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        {
            let mut relatives=Vec::new();
            // repair_missing_context adds all zombie edges needed.
            let mut repair_missing_context= |repo:&mut Repository,direction_up:bool,c:&[u8] | {
                let mut context:[u8;KEY_SIZE]=[0;KEY_SIZE];
                unsafe {
                    let u= if c.len()>LINE_SIZE {
                        repo.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                    } else {
                        internal as &[u8]
                    };
                    copy_nonoverlapping(u.as_ptr(), context.as_mut_ptr(), HASH_SIZE);
                    copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                        context.as_mut_ptr().offset(HASH_SIZE as isize),
                                        LINE_SIZE);
                }
                debug!(target:"missing context","{} context:{}",direction_up,to_hex(&context[..]));
                if if direction_up { !has_edge(cursor,&context[..],PARENT_EDGE,true,true) } else { has_edge(cursor,&context[..],PARENT_EDGE|DELETED_EDGE,true,true) } {
                    relatives.clear();
                    repo.find_alive_relatives(&context[..],
                                              if direction_up {DELETED_EDGE|PARENT_EDGE} else {DELETED_EDGE},
                                              internal,new_patches,
                                              &mut relatives);
                    debug!(target:"missing context","up relatives:{} {}",to_hex(&relatives),relatives.len());
                    let mut i=0;
                    while i<relatives.len() {
                        debug!(target:"missing context",
                               "relatives:\n  {}\n  {}",
                               to_hex(&relatives[(i)..(i+EDGE_SIZE)]),
                               to_hex(&relatives[(i+EDGE_SIZE)..(i+2*EDGE_SIZE)]));
                        repo.mdb_txn.put(repo.dbi_nodes,
                                         &relatives[(i+1)..(i+1+KEY_SIZE)],
                                         &relatives[(i+EDGE_SIZE)..(i+2*EDGE_SIZE)],
                                         0).unwrap();
                        repo.mdb_txn.put(repo.dbi_nodes,
                                         &relatives[(i+EDGE_SIZE+1)..(i+EDGE_SIZE+1+KEY_SIZE)],
                                         &relatives[i..(i+EDGE_SIZE)],
                                         0).unwrap();
                        i+=2*EDGE_SIZE
                    }
                }
            };

            let mut u=[0;KEY_SIZE];
            let mut v=[0;KEY_SIZE];
            for ch in patch.changes.iter() {
                match *ch {
                    Change::Edges{ref flag,ref edges}=>{

                        if (*flag)&DELETED_EDGE == 0 {
                            // Handle missing context (up and down)
                            // Untested (how to generate non-deleted Change::Edges?)
                            for e in edges {
                                {
                                    let int_from=try!(self.internal_hash(&e.from[0..(e.from.len()-LINE_SIZE)]));
                                    let int_to=try!(self.internal_hash(&e.to[0..(e.to.len()-LINE_SIZE)]));
                                    unsafe {
                                        copy_nonoverlapping(int_from.as_ptr(),u.as_mut_ptr(),HASH_SIZE);
                                        copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                                            u.as_mut_ptr().offset(HASH_SIZE as isize),
                                                            LINE_SIZE);
                                        copy_nonoverlapping(int_to.as_ptr(),v.as_mut_ptr(),HASH_SIZE);
                                        copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                                            v.as_mut_ptr().offset(HASH_SIZE as isize),
                                                            LINE_SIZE);
                                    }
                                }
                                repair_missing_context(self,(*flag)&PARENT_EDGE != 0,&u[..]);
                                repair_missing_context(self,(*flag)&PARENT_EDGE == 0,&v[..]);
                            }
                        } else // DELETED_EDGE
                            if (*flag) & FOLDER_EDGE != 0 {
                                for e in edges {
                                    {
                                        let dest= if *flag & PARENT_EDGE != 0 { &e.from } else { &e.to };
                                        let int_dest=try!(self.internal_hash(&dest[0..(dest.len()-LINE_SIZE)]));
                                        unsafe {
                                            copy_nonoverlapping(int_dest.as_ptr(),u.as_mut_ptr(),HASH_SIZE);
                                            copy_nonoverlapping(dest.as_ptr().offset((dest.len()-LINE_SIZE) as isize),
                                                                u.as_mut_ptr().offset(HASH_SIZE as isize),
                                                                LINE_SIZE);
                                        }
                                    }
                                    let u_is_empty=
                                        match try!(self.mdb_txn.get(self.dbi_contents,&u[..])) {
                                            Some(cont)=>cont.len()==0,
                                            None=>true
                                        };
                                    if u_is_empty && has_edge(cursor_,&u[..],0,true,true) {
                                        // If a deleted folder edge has alive children, reconnect it to the root.
                                        try!(self.reconnect_zombie_folder(&u[..],internal));
                                    }
                                }
                            }
                    },
                    Change::NewNodes { ref up_context,ref down_context, .. } => {
                        // Handle missing contexts.
                        for c in up_context {
                            repair_missing_context(self,true,c)
                        }
                        for c in down_context {
                            repair_missing_context(self,false,c)
                        }
                        debug!(target:"libpijul","apply: newnodes, done");
                    }
                }
            }
        }
        unsafe {
            lmdb::mdb_cursor_close(cursor);
            lmdb::mdb_cursor_close(cursor_);
        }
        let time2=time::precise_time_s();
        for ref dep in patch.dependencies.iter() {
            let dep_internal=try!(self.internal_hash(&dep)).to_vec();
            self.mdb_txn.put(self.dbi_revdep,&dep_internal,internal,0).unwrap();
        }
        let time3=time::precise_time_s();
        info!(target:"libpijul","deps took: {}", time3-time2);
        Ok(())
    }


    fn find_alive_relatives(&self, a:&[u8], direction:u8, patch_id:&[u8], new_patches:&HashSet<&[u8]>,
                            relatives:&mut Vec<u8>) {
        let cursor= unsafe { &mut * self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        fn connect(repo:&Repository,
                   cursor:&mut lmdb::MdbCursor,
                   a:&[u8],
                   direction:u8,
                   result:&mut Vec<u8>,
                   //buffer:&mut Vec<u8>,
                   patch_id:&[u8],
                   new_patches:&HashSet<&[u8]>) {
            // different from root
            if unsafe { memcmp(ROOT_KEY.as_ptr()as *const c_void,a.as_ptr() as *const c_void,KEY_SIZE as size_t)!=0} {

                let mut i=result.len();
                for neighbor in CursIter::new(cursor,a,direction,true,false) {
                    // Is this neighbor from one of the newly applied patches?
                    let is_new=
                        if unsafe { memcmp(neighbor.as_ptr().offset(1+KEY_SIZE as isize) as *const c_void,
                                           patch_id.as_ptr() as *const c_void,
                                           HASH_SIZE as size_t) == 0 } {
                            false
                        } else {
                            let ext=repo.external_hash(&neighbor[(1+KEY_SIZE)..]);
                            new_patches.contains(ext)
                        };
                    if is_new {
                        result.push((neighbor[0]^PARENT_EDGE)^DELETED_EDGE);
                        result.extend(a);
                        result.extend(patch_id);

                        result.push(neighbor[0]^DELETED_EDGE);
                        result.extend(&neighbor[1..(1+KEY_SIZE)]);
                        result.extend(patch_id);
                    }
                }
                let j=result.len();
                debug_assert!(a.len()==KEY_SIZE);
                debug_assert!(patch_id.len()==HASH_SIZE);
                debug!(target:"alive","a={}",to_hex(a));
                let mut copy=[0;KEY_SIZE];
                while i < j {
                    unsafe {
                        copy_nonoverlapping(result.as_ptr().offset((i+2+KEY_SIZE+HASH_SIZE) as isize),
                                            copy.as_mut_ptr(),
                                            KEY_SIZE);
                    }
                    connect(repo,cursor, &copy[..],direction,result,
                            //buffer,
                            patch_id,new_patches);
                    i+= 2*(1+KEY_SIZE+HASH_SIZE)
                }
                //buffer.truncate(i0)
            }
        }
        //let mut buf=Vec::with_capacity(4*KEY_SIZE);
        connect(self,cursor,a,direction,relatives,patch_id,new_patches);
        unsafe { lmdb::mdb_cursor_close(cursor); }
    }


    fn reconnect_zombie_folder(&mut self, a:&[u8], patch_id:&[u8])->Result<(),Error> {
        fn connect(repo:&Repository,
                   cursor:&mut lmdb::MdbCursor,
                   a:&[u8],
                   patch_id:&[u8],
                   edges:&mut Vec<u8>) {
            debug!(target:"missing context","connect zombie: {} {}",to_hex(a),
                   has_edge(cursor,&a,PARENT_EDGE|FOLDER_EDGE,true,false));
            if unsafe { memcmp(ROOT_KEY.as_ptr()as *const c_void,a.as_ptr() as *const c_void,KEY_SIZE as size_t)!=0}
            && !has_edge(cursor,&a,PARENT_EDGE|FOLDER_EDGE,true,false) {
                let i=edges.len();
                for neighbor in CursIter::new(cursor,&a,PARENT_EDGE|DELETED_EDGE|FOLDER_EDGE,true,false) {
                    debug!(target:"missing context","pushing from {}",to_hex(a));
                    debug!(target:"missing context","pushing {}",to_hex(neighbor));
                    edges.push(FOLDER_EDGE);
                    edges.extend(a);
                    edges.extend(patch_id);
                    edges.push(PARENT_EDGE|FOLDER_EDGE);
                    edges.extend(&neighbor[1..(1+KEY_SIZE)]);
                    edges.extend(patch_id);
                }
                let mut j=i;
                let l=edges.len();
                let mut neighbor=[0;KEY_SIZE];
                while j<l {
                    unsafe {
                        copy_nonoverlapping(edges.as_ptr().offset((j+EDGE_SIZE+1) as isize),
                                            neighbor.as_mut_ptr(),
                                            KEY_SIZE)
                    }
                    connect(repo,cursor,&neighbor[..],patch_id,edges);
                    j+=2*EDGE_SIZE
                }
            }
            debug!(target:"missing context","/connect zombie: {}",to_hex(a));
        }
        //let mut buf=Vec::with_capacity(4*KEY_SIZE);
        let mut edges=Vec::new();
        let cursor= unsafe { &mut * self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        connect(self,cursor,a,patch_id,&mut edges);
        debug!(target:"missing context","edges.len()={}",edges.len());
        let mut i=0;
        while i<edges.len() {
            try!(self.mdb_txn.put(self.dbi_nodes,&edges[(i+1)..(i+1+KEY_SIZE)],
                                  &edges[(i+EDGE_SIZE)..(i+2*EDGE_SIZE)],
                                  lmdb::MDB_NODUPDATA));
            try!(self.mdb_txn.put(self.dbi_nodes,&edges[(i+EDGE_SIZE+1)..(i+EDGE_SIZE+1+KEY_SIZE)],
                                  &edges[i..(i+EDGE_SIZE)],
                                  lmdb::MDB_NODUPDATA));
            i+=2*EDGE_SIZE
        }
        unsafe { lmdb::mdb_cursor_close(cursor); }
        Ok(())
    }



    pub fn write_changes_file(&self,changes_file:&Path)->Result<(),Error> {
        let mut patches=HashSet::new();
        let branch=self.get_current_branch();
        let curs=self.mdb_txn.cursor(self.dbi_branches).unwrap();
        let mut op=lmdb::Op::MDB_SET;
        while let Ok((_,v))=curs.get(&branch,None,op) {
            patches.insert(self.external_hash(v));
            op=lmdb::Op::MDB_NEXT_DUP
        }
        try!(patch::write_changes(&patches,changes_file));
        Ok(())
    }




    pub fn sync_file_additions(&mut self, changes:&[Change], updates:&HashMap<LocalKey,Inode>, internal_patch_id:&[u8]){
        let mut node=[0;3+KEY_SIZE];
        let mut node_=[0;3+KEY_SIZE];
        let mut inode=[0;INODE_SIZE];
        let mut cursor= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        for change in changes {
            match *change {
                Change::NewNodes { ref line_num,ref flag,ref nodes,.. } => {
                    if flag&FOLDER_EDGE != 0 {
                        debug!(target:"sync_file_additions","nodes.len()={}",nodes.len());
                        unsafe { let _=copy_nonoverlapping(internal_patch_id.as_ptr(), node.as_mut_ptr().offset(3), HASH_SIZE); }
                        let mut l0=*line_num + 1;
                        for i in 0..LINE_SIZE { node[3+HASH_SIZE+i]=(l0&0xff) as u8; l0 = l0>>8 }
                        match updates.get(&node[(3+HASH_SIZE)..]) {
                            None => {
                                // This file comes from a remote patch
                                self.create_new_inode(&mut inode);
                                debug!(target:"sync_file_additions","create {}",to_hex(&inode[..]));
                            },
                            Some(ref up_inode)=> {
                                // This file comes from a local patch
                                debug!(target:"sync_file_additions","needs update {}",to_hex(&up_inode[..]));
                                unsafe {
                                    copy_nonoverlapping(up_inode.as_ptr(),inode.as_mut_ptr(),INODE_SIZE);
                                }
                            }
                        };
                        //println!("adding inode: {:?} for node {:?}",inode,node);
                        node[0]=0;
                        node[1]=(nodes[0][0] & 0xff) as u8;
                        node[2]=(nodes[0][1] & 0xff) as u8;
                        self.mdb_txn.put(self.dbi_inodes,&inode,&node,0).unwrap();
                        self.mdb_txn.put(self.dbi_revinodes,&node[3..],&inode,0).unwrap();
                    }
                },
                Change::Edges{ ref flag, ref edges} => {
                    if flag&FOLDER_EDGE != 0 {
                        for e in edges {
                            {
                                let no= if flag&PARENT_EDGE==0 { &e.to } else { &e.from };
                                let to= self.internal_hash(&no[0..(no.len()-LINE_SIZE)]).unwrap();
                                debug!(target:"sync","to={}",to_hex(&to));
                                unsafe {
                                    copy_nonoverlapping(to.as_ptr(),node.as_mut_ptr().offset(3),HASH_SIZE);
                                    copy_nonoverlapping(no.as_ptr().offset((no.len()-LINE_SIZE) as isize),
                                                        node.as_mut_ptr().offset((3+HASH_SIZE) as isize),
                                                        LINE_SIZE);
                                }
                            }
                            let mut unique_child= true;
                            for child in CursIter::new(cursor,&node[3..],FOLDER_EDGE,true,false) {
                                // Invariant: there "should be" only one child here.
                                debug_assert!(unique_child);
                                unique_child=false;
                                //
                                let needs_update= {
                                    match self.mdb_txn.get(self.dbi_revinodes,&child[1..(1+KEY_SIZE)]).unwrap() {
                                        None=>false,
                                        Some(inode_)=> {
                                            let old_node = self.mdb_txn.get(self.dbi_inodes,&inode_).unwrap().unwrap();
                                            unsafe {
                                                copy_nonoverlapping(inode_.as_ptr(),
                                                                    inode.as_mut_ptr(),
                                                                    INODE_SIZE);
                                                copy_nonoverlapping(old_node.as_ptr(),
                                                                    node_.as_mut_ptr(),
                                                                    3+KEY_SIZE);
                                                node_[0]=0;
                                            }
                                            true
                                        }
                                    }
                                };
                                if needs_update {
                                    self.mdb_txn.put(self.dbi_inodes,&inode,&node_,0).unwrap();
                                    self.mdb_txn.put(self.dbi_revinodes,&node_[3..],&inode,0).unwrap();
                                }
                            }
                        }
                    }
                }
            }
        }
        unsafe {
            lmdb::mdb_cursor_close(cursor);
        }
    }




    /// Assumes all patches have been downloaded. The third argument
    /// `remote_patches` needs to contain at least all the patches we want
    /// to apply, and the fourth one `local_patches` at least all the patches the other
    /// party doesn't have.
    pub fn apply_patches(&mut self,
                         r:&Path,
                         remote_patches:&HashSet<Vec<u8>>,
                         local_patches:&HashSet<Vec<u8>>) -> Result<(), Error> {
        debug!("local {}, remote {}",local_patches.len(),remote_patches.len());
        let pullable=remote_patches.difference(&local_patches);
        let only_local={
            let mut only_local:HashSet<&[u8]>=HashSet::new();
            for i in local_patches.difference(&remote_patches) { only_local.insert(&i[..]); };
            only_local
        };
        fn apply_patches<'a>(repo:&mut Repository<'a>, branch:&[u8], local_patches:&Path, patch_hash:&[u8], patches_were_applied:&mut bool, only_local:&HashSet<&[u8]>)->Result<(),Error>{
            if !try!(repo.has_patch(branch,patch_hash)) {
                let local_patch=local_patches.join(to_hex(patch_hash)).with_extension("cbor");
                debug!("local_patch={:?}",local_patch);
                let mut buffer = BufReader::new(try!(File::open(local_patch)));
                let patch=try!(Patch::from_reader(&mut buffer));
                for dep in patch.dependencies.iter() {
                    try!(apply_patches(repo,branch,local_patches,&dep,patches_were_applied, only_local))
                }
                let mut internal=[0;HASH_SIZE];
                repo.new_internal(&mut internal);
                //println!("pulling and applying patch {}",to_hex(patch_hash));
                try!(repo.apply(&patch, &internal,only_local));
                *patches_were_applied=true;
                repo.sync_file_additions(&patch.changes[..],&HashMap::new(), &internal);
                repo.register_hash(&internal[..],patch_hash);
                Ok(())
            } else {
                Ok(())
            }
        }
        let local_patches=patches_dir(r);
        let current_branch=self.get_current_branch().to_vec();
        let pending={
            let (changes,_)= try!(self.record(&r));
            let mut p=Patch::empty();
            p.changes=changes;
            p
        };
        let mut patches_were_applied=false;
        for p in pullable {
            try!(apply_patches(self,&current_branch,&local_patches,p,&mut patches_were_applied,&only_local))
        }
        debug!(target:"pull","patches applied? {}",patches_were_applied);
        if patches_were_applied {
            try!(self.write_changes_file(&branch_changes_file(r,&current_branch)));
            debug!(target:"pull","output_repository");
            try!(self.output_repository(&r,&pending))
        }
        if cfg!(debug_assertions){
            let mut buffer = BufWriter::new(File::create(r.join("debug")).unwrap());
            self.debug(&mut buffer);
        }
        Ok(())
    }








    // Climp up the tree (using revtree).
    fn filename_of_inode(&self,inode:&[u8],working_copy:&mut PathBuf)->bool {
        //let mut v_inode=MDB_val{mv_data:inode.as_ptr() as *const c_void, mv_size:inode.len() as size_t};
        //let mut v_next:MDB_val = unsafe {std::mem::zeroed()};
        let mut components=Vec::new();
        let mut current=inode;
        loop {
            match self.mdb_txn.get(self.dbi_revtree,current) {
                Ok(Some(v))=>{
                    components.push(&v[INODE_SIZE..]);
                    current=&v[0..INODE_SIZE];
                    if unsafe { memcmp(current.as_ptr() as *const c_void,
                                       ROOT_INODE.as_ptr() as *const c_void,
                                       INODE_SIZE as size_t) } == 0 {
                        break
                    }
                },
                Ok(None)=> return false,
                Err(_)=>panic!("filename_of_inode")
            }
        }
        for c in components.iter().rev() {
            working_copy.push(std::str::from_utf8(c).unwrap());
        }
        true
    }

    pub fn retrieve_paths<'b>(&'b self, key:&[u8], flag:u8) -> Result<Vec<(usize,&'b [u8],&'b[u8])>,Error> {
        let mut result=Vec::new();
        let mut curs_b= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()};
        for b in CursIter::new(curs_b,key,flag,true,true) {
            debug!(target:"retrieve_paths","b={:?}",to_hex(b));
            let cont_b=
                match try!(self.mdb_txn.get(self.dbi_contents,&b[1..(1+KEY_SIZE)])) {
                    Some(cont_b)=>cont_b,
                    None=>&[][..]
                };
            if cont_b.len()<2 {
                //panic!("node (b) too short")
            } else {
                let filename=&cont_b[2..];
                let perms= ((cont_b[0] as usize) << 8) | (cont_b[1] as usize);
                let mut curs_c= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()};
                for c in CursIter::new(curs_c,&b[1..(1+KEY_SIZE)],flag,true,true) {
                    debug!(target:"retrieve_paths","c={:?}",to_hex(c));
                    let cv=&c[1..(1+KEY_SIZE)];

                    result.push((perms,filename,cv))
                }
            }
        }
        Ok(result)
    }

    /// Returns the path's inode
    pub fn follow_path<'b>(&'b self, path:&[&[u8]])->Result<Option<Vec<u8>>,Error> {
        // follow in tree, return inode
        let mut buf=vec![0;INODE_SIZE];
        let mut first=true;
        for p in path {
            buf.extend(*p);
            println!("follow: {:?}",buf);
            match try!(self.mdb_txn.get(self.dbi_tree,&buf)) {
                Some(v)=> {
                    println!("some: {:?}",buf);
                    first=false;
                    buf.clear();
                    buf.extend(v)
                },
                None => {
                    println!("none");
                    if first { buf.truncate(INODE_SIZE); return Ok(Some(buf)) } else { return Ok(None) }
                }
            }
        }
        Ok(Some(buf))
    }
    /// Returns the node's properties
    pub fn node_of_inode<'b>(&'b self, inode:&[u8])->Result<Option<&'b [u8]>,Error> {
        // follow in tree, return inode
        if unsafe { memcmp(inode.as_ptr() as *const c_void,
                           ROOT_INODE.as_ptr() as *const c_void,
                           INODE_SIZE as size_t) } == 0 {
            Ok(Some(ROOT_KEY))
        } else {
            let node=try!(self.mdb_txn.get(self.dbi_inodes,&inode));
            Ok(node)
        }
    }


    fn unsafe_output_repository(&mut self, working_copy:&Path) -> Result<Vec<(Vec<u8>,Vec<u8>)>,Error>{
        fn retrieve_paths<'a> (repo:&'a Repository,
                               working_copy:&Path,
                               key:&'a [u8],path:&mut PathBuf,parent_inode:&'a [u8],
                               paths:&mut BTreeMap<PathBuf,Vec<(Vec<u8>,Vec<u8>,Vec<u8>,Option<PathBuf>,usize)>>,
                               cache:&mut HashSet<&'a [u8]>,
                               nfiles:&mut usize)-> Result<(),Error> {
            if !cache.contains(key) {
                cache.insert(key);
                let mut curs_b= unsafe { &mut *repo.mdb_txn.unsafe_cursor(repo.dbi_nodes).unwrap()};
                debug!(target:"output_repository","key={}",to_hex(key));
                for b in CursIter::new(curs_b,key,FOLDER_EDGE,true,true) {
                    debug!(target:"output_repository","b={}",to_hex(b));
                    let cont_b=
                        match try!(repo.mdb_txn.get(repo.dbi_contents,&b[1..(1+KEY_SIZE)])) {
                            Some(cont_b)=>cont_b,
                            None=>&[][..]
                        };
                    debug!(target:"output_repository","path:{:?} cont_b={}",path,std::str::from_utf8(&cont_b[2..]).unwrap());
                    if cont_b.len()<2 { panic!("node (b) too short") } else {
                        let filename=&cont_b[2..];
                        let perms= ((cont_b[0] as usize) << 8) | (cont_b[1] as usize);
                        let mut curs_c= unsafe { &mut *repo.mdb_txn.unsafe_cursor(repo.dbi_nodes).unwrap()};
                        for c in CursIter::new(curs_c,&b[1..(1+KEY_SIZE)],FOLDER_EDGE,true,true) {
                            debug!(target:"output_repository","c={}",to_hex(c));
                            let cv=&c[1..(1+KEY_SIZE)];
                            match try!(repo.mdb_txn.get(repo.dbi_revinodes,cv)) {
                                Some(inode)=>{
                                    path.push(std::str::from_utf8(filename).unwrap());
                                    {
                                        let vec=paths.entry(path.clone()).or_insert(Vec::new());
                                        let mut buf=PathBuf::from(working_copy);
                                        *nfiles+=1;
                                        vec.push((cv.to_vec(),parent_inode.to_vec(),inode.to_vec(),
                                                  if repo.filename_of_inode(inode,&mut buf) {Some(buf)} else { None },
                                                  perms))
                                    }
                                    if perms & DIRECTORY_FLAG != 0 { // is_directory
                                        try!(retrieve_paths(repo,working_copy,cv,path,inode,paths,cache,nfiles));
                                    }
                                    path.pop();
                                },
                                None =>{
                                    panic!("inodes not synchronized")
                                }
                            }
                        }
                        unsafe { lmdb::mdb_cursor_close(curs_c) }
                    }
                    debug!(target:"output_repository","/b");
                }
                unsafe { lmdb::mdb_cursor_close(curs_b) }
            }
            Ok(())
        }
        let mut paths=BTreeMap::new();
        let mut nfiles=0;
        {
            let mut cache=HashSet::new();
            let mut buf=PathBuf::from(working_copy);
            try!(retrieve_paths(self,working_copy,&ROOT_KEY,&mut buf,ROOT_INODE,&mut paths,&mut cache,&mut nfiles));
        }
        debug!(target:"output_repository","collection done");
        //println!("dropping tree");
        {
            try!(self.mdb_txn.drop(self.dbi_tree,false));
            try!(self.mdb_txn.drop(self.dbi_revtree,false));
        };
        let mut updates=Vec::with_capacity(nfiles);
        let mut new_paths:HashMap<&Path,PathBuf>=HashMap::new();
        // First pass: create and move everything needed.
        for (k,a) in paths.iter() {
            let alen=a.len();
            debug!(target:"output_repository","output: k={:?}, alen={}",k,alen);
            let mut i=0;
            for x in a.iter() {
                let (_,ref parent_inode,ref inode,ref oldpath,ref perms) = *x;
                let mut filename=k.file_name().unwrap().to_os_string();
                if alen>1 { filename.push(format!("~{}",i)) }
                let mut kk=k.clone();
                debug!(target:"output_repository","output: {:?}",filename);
                kk.set_file_name(&filename);
                match *oldpath {
                    Some(ref oldpath) if i==0 => {
                        debug!(target:"output_repository","oldpath: {:?}, newpath: {:?}",oldpath,kk);
                        {
                            let oldpath_=oldpath.as_path();
                            debug!(target:"output_repository","oldpath.parent(): {:?} {:?}",oldpath.parent(),new_paths.get(oldpath.parent().unwrap()));
                            let oldpath_=match new_paths.get(oldpath.parent().unwrap()) {
                                None=>PathBuf::from(oldpath_),
                                Some(dirname)=>{
                                    let base:&std::ffi::OsStr=oldpath.file_name().unwrap();
                                    dirname.as_path().join(base)
                                }
                            };
                            debug!(target:"output_repository","oldpath_: {:?}",oldpath_);
                            let _ = fs::rename(&oldpath_,&kk).map_err(Error::IoError);
                        }
                        new_paths.insert(oldpath,kk);
                    },
                    _ =>{
                        if perms & DIRECTORY_FLAG != 0 {
                            try!(std::fs::create_dir_all(&k).map_err(Error::IoError));
                        }
                    }
                }
                let mut par=parent_inode.to_vec();
                par.extend(filename.to_str().unwrap().as_bytes());
                updates.push((par,inode.to_vec()));
                i+=1
            }
        }
        debug!(target:"output_repository","now outputting files");
        for (k,a) in paths.iter() {
            let alen=a.len();
            debug!(target:"output_repository","output: k={:?}, alen={}",k,alen);
            let mut i=0;
            for x in a.iter() {
                let mut filename=k.file_name().unwrap().to_os_string();
                if alen>1 { filename.push(format!("~{}",i)) }
                let mut kk=k.clone();
                kk.set_file_name(&filename);
                let (ref node,_,_,_,ref perms) = *x;
                // Then (if file) output file
                if perms & DIRECTORY_FLAG == 0 { // this is a real file, not a directory
                    let mut redundant_edges=vec!();
                    {
                        let time0=time::precise_time_s();
                        let l=self.retrieve(&node).unwrap();
                        let time1=time::precise_time_s();
                        info!(target:"output_repository","unsafe_output_repository: retrieve took {}s", time1-time0);
                        debug!(target:"output_repository","create file: {:?}",kk);
                        if fs::metadata(k.parent().unwrap()).is_err() {
                            println!("Output repository: there is probably a name conflict on directory {:?} and something else, this version of Pijul doesn't say (even though it would be easy).", kk.parent().unwrap());
                            try!(std::fs::create_dir_all(kk.parent().unwrap()).map_err(Error::IoError));
                        }
                        let mut f=std::fs::File::create(&kk).unwrap();
                        self.output_file(&mut f,l,&mut redundant_edges);
                        let time2=time::precise_time_s();
                        info!(target:"output_repository","unsafe_output_repository: output_file took {}s", time2-time1);
                    }
                    self.remove_redundant_edges(&mut redundant_edges);
                }
                i+=1
            }
        }
        debug!(target:"output_repository","unsafe_output done");
        Ok(updates)
    }

    pub fn retrieve_and_output<L:LineBuffer<'a>>(&'a self,key:&'a [u8],l:&mut L) {
        let mut redundant_edges=vec!();
        let graph=self.retrieve(key).unwrap();
        self.output_file(l,graph,&mut redundant_edges);
    }

    /// This is relatively suboptimal right now: in the future, we should carefully delete outdated parts of the tree database in unsafe_output_repository, and then reinsert them here.
    fn update_tree(&mut self,updates:Vec<(Vec<u8>,Vec<u8>)>)->Result<(),Error>{
        try!(self.mdb_txn.drop(self.dbi_tree,false));
        try!(self.mdb_txn.drop(self.dbi_revtree,false));
        for (par,inode) in updates {
            debug!(target:"output_repository","update_tree:{},{},{}",
                   to_hex(&par[0..INODE_SIZE]),
                   to_hex(&inode),
                   std::str::from_utf8(&par[INODE_SIZE..]).unwrap());
            try!(self.mdb_txn.put(self.dbi_tree,&par,&inode,0));
            try!(self.mdb_txn.put(self.dbi_revtree,&inode,&par,0));
        }
        Ok(())
    }


    pub fn register_patch(&mut self, location: &Path, patch: Patch, inode_updates:&HashMap<LocalKey,Inode>)
                          -> Result<(), Error>{
        info!("registering a patch with {} changes", patch.changes.len());
        let patch = Arc::new(patch);
        let child_patch = patch.clone();
        let patches_dir = patches_dir(location);
        let hash_child = thread::spawn(move || {
            let t0 = time::precise_time_s();
            let hash = child_patch.save(&patches_dir);
            let t1 = time::precise_time_s();
            info!("hashed patch in {}s", t1-t0);
            hash
        });

        let t0 = time::precise_time_s();

        let mut internal=[0;HASH_SIZE];
        self.new_internal(&mut internal);
        debug!(target:"pijul", "applying patch");
        try!(self.apply(&patch, &internal, &HashSet::new()));
        debug!(target:"pijul", "synchronizing tree");
        self.sync_file_additions(&patch.changes[..], &inode_updates, &internal);
        if cfg!(debug_assertions){
            let mut buffer = BufWriter::new(File::create(location.join("debug")).unwrap());
            self.debug(&mut buffer);
        }
        let t2 = time::precise_time_s();
        info!("applied patch in {}s", t2 - t0);

        match hash_child.join() {
            Ok(Ok(hash))=> {
                self.register_hash(&internal[..],&hash[..]);
                debug!(target:"record","hash={}, local={}",to_hex(&hash),to_hex(&internal));
                //println!("writing changes {:?}",internal);
                self.write_changes_file(&branch_changes_file(location,self.get_current_branch())).unwrap();
                let t3=time::precise_time_s();
                info!("changes files took {}s to write", t3-t2);
                Ok(())
            },
            Ok(Err(x)) => {
                Err(x)
            },
            Err(_)=>{
                panic!("saving patch")
            }
        }
    }

    pub fn output_repository(&mut self, working_copy:&Path, pending:&Patch) -> Result<(),Error>{
        debug!(target:"output_repository","begin output repository");
        unsafe {
            let mut internal=[0;HASH_SIZE];
            let parent_txn=self.mdb_txn.txn;
            let txn=ptr::null_mut();

            let e=lmdb::mdb_txn_begin(self.mdb_env.env,self.mdb_txn.txn,0,std::mem::transmute(&txn));
            if e!=0 {
                return Err(Error::IoError(std::io::Error::from_raw_os_error(e)))
            }
            self.mdb_txn.txn=txn;
            self.new_internal(&mut internal[..]);
            debug!(target:"output_repository","pending patch: {}",to_hex(&internal[..]));
            self.apply(pending,&internal[..],&HashSet::new()).unwrap();
            let updates=try!(self.unsafe_output_repository(working_copy));
            lmdb::mdb_txn_abort(txn);
            self.mdb_txn.txn=parent_txn;
            try!(self.update_tree(updates));
            debug!(target:"output_repository","end output repository");
            Ok(())
        }
    }


    pub fn debug<W>(&mut self,w:&mut W) where W:Write {
        let mut styles=Vec::with_capacity(16);
        for i in 0..16 {
            styles.push(("color=").to_string()
                        +["red","blue","green","black"][(i >> 1)&3]
                        +if (i as u8)&DELETED_EDGE!=0 { ", style=dashed"} else {""}
                        +if (i as u8)&PSEUDO_EDGE!=0 { ", style=dotted"} else {""})
        }
        w.write(b"digraph{\n").unwrap();
        let curs=self.mdb_txn.cursor(self.dbi_nodes).unwrap();
        let mut op=lmdb::Op::MDB_FIRST;
        let mut cur=&[][..];
        while let Ok((k,v))=curs.get(cur,None,op) {
            op=lmdb::Op::MDB_NEXT;
            if k!=cur {
                let f=self.mdb_txn.get(self.dbi_contents, k);
                let cont:&[u8]=
                    match f {
                        Ok(Some(ww))=>ww,
                        _=>&[]
                    };
                write!(w,"n_{}[label=\"{}: {}\"];\n", to_hex(&k), to_hex(&k),
                       match str::from_utf8(&cont) { Ok(x)=>x.to_string(), Err(_)=> to_hex(&cont) }
                       ).unwrap();
                cur=k;
            }
            let flag=v[0];
            if true || flag & PARENT_EDGE == 0 {
                write!(w,"n_{}->n_{}[{},label=\"{}\"];\n", to_hex(&k), to_hex(&v[1..(1+KEY_SIZE)]), styles[(flag&0xff) as usize], flag).unwrap();
            }
        }
        w.write(b"}\n").unwrap();
    }
}
/*
fn dump_table(txn:*mut MdbTxn,dbi:MdbDbi){
    println!("dumping table");
    unsafe {
        let mut k:MDB_val=std::mem::zeroed();
        let mut v:MDB_val=std::mem::zeroed();
        let c=Cursor::new(txn,dbi).unwrap();
        let mut e=mdb_cursor_get(c.cursor,&mut k,&mut v,Op::MDB_FIRST as c_uint);
        while e==0 {
            let kk=std::slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize);
            let vv=std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize);
            println!("key:{:?}, value={:?}",to_hex(kk),to_hex(vv));
            e=mdb_cursor_get(c.cursor,&mut k,&mut v,Op::MDB_NEXT as c_uint)
        }
    }
    println!("/dumping table");
}
*/
