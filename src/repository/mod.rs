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
use self::libc::{c_int, c_uint,c_char,c_uchar,c_void,size_t};
use self::libc::{memcmp};
use std::ptr::{copy_nonoverlapping};
use std::ptr;

use std::slice;
use std::str;
use std;
use std::collections::HashMap;
extern crate rand;
use std::path::{PathBuf,Path};

use std::io::prelude::*;
use std::io::Error;
use std::collections::HashSet;
use std::fs::{metadata};
pub mod fs_representation;
pub mod patch;

use std::os::unix::fs::PermissionsExt;

use self::patch::{Change,Edge,LocalKey,ExternalKey};


extern crate rustc_serialize;

use self::rustc_serialize::hex::{ToHex};
use std::fs;

mod mdb;
use self::mdb::*;

/// The repository structure, on which most functions work.
pub struct Repository{
    mdb_env:*mut MdbEnv,
    mdb_txn:*mut MdbTxn,
    dbi_nodes:MdbDbi,
    dbi_revdep:MdbDbi,
    dbi_contents:MdbDbi,
    dbi_internal:MdbDbi,
    dbi_external:MdbDbi,
    dbi_branches:MdbDbi,
    dbi_tree:MdbDbi,
    dbi_revtree:MdbDbi,
    dbi_inodes:MdbDbi,
    dbi_revinodes:MdbDbi
}

impl Repository {
    pub fn new(path:&std::path::Path)->Result<Repository,Error>{
        unsafe {
            let env=ptr::null_mut();
            let e=mdb_env_create(std::mem::transmute(&env));
            if e != 0 { println!("mdb_env_create");
                        return Err(Error::from_raw_os_error(e)) };
            let mut dead:c_int=0;
            let e=mdb_reader_check(env,&mut dead);
            if e != 0 { println!("mdb_reader_check");return Err(Error::from_raw_os_error(e)) };
            let e=mdb_env_set_maxdbs(env,10);
            if e != 0 { println!("mdb_env_set_maxdbs");return Err(Error::from_raw_os_error(e)) };
            let e=mdb_env_set_mapsize(env,std::ops::Shl::shl(1,30) as size_t);
            if e !=0 { println!("mdb_env_set_mapsize");return Err(Error::from_raw_os_error(e)) };
            let p=path.as_os_str().to_str();
            match p {
                Some(pp) => {
                    let e=mdb_env_open(env,pp.as_ptr() as *const i8,0,0o755);
                    if e !=0 { println!("mdb_env_open");return Err(Error::from_raw_os_error(e)) };

                    let txn=ptr::null_mut();
                    let e=mdb_txn_begin(env,ptr::null_mut(),0,std::mem::transmute(&txn));
                    if e !=0 { println!("mdb_env_open");return Err(Error::from_raw_os_error(e)) };
                    fn open_dbi(txn:*mut MdbTxn,name:&str,flag:c_uint)->MdbDbi {
                        let mut d=0;
                        let e=unsafe { mdb_dbi_open(txn,name.as_ptr() as *const c_char,flag,&mut d) };
                        if e==0 { d } else {
                            panic!("Database could not be opened")
                        }
                    }
                    let repo=Repository{
                        mdb_env:env,
                        mdb_txn:txn,
                        dbi_nodes:open_dbi(txn,"nodes\0",MDB_CREATE|MDB_DUPSORT),
                        dbi_revdep:open_dbi(txn,"revdep\0",MDB_CREATE|MDB_DUPSORT),
                        dbi_contents:open_dbi(txn,"contents\0",MDB_CREATE),
                        dbi_internal:open_dbi(txn,"internal\0",MDB_CREATE),
                        dbi_external:open_dbi(txn,"external\0",MDB_CREATE),
                        dbi_branches:open_dbi(txn,"branches\0",MDB_CREATE|MDB_DUPSORT),
                        dbi_tree:open_dbi(txn,"tree\0",MDB_CREATE|MDB_DUPSORT),
                        dbi_revtree:open_dbi(txn,"revtree\0",MDB_CREATE),
                        dbi_inodes:open_dbi(txn,"inodes\0",MDB_CREATE),
                        dbi_revinodes:open_dbi(txn,"revinodes\0",MDB_CREATE),
                    };
                    Ok(repo)
                },
                None => {
                    println!("invalid path");
                    Err(Error::from_raw_os_error(0))
                }
            }
        }
    }
}

impl Drop for Repository {
    fn drop(&mut self){
        unsafe {
            if std::thread::panicking() {
                mdb_txn_abort(self.mdb_txn);
            } else {
                mdb_txn_commit(self.mdb_txn);
            }
            mdb_env_close(self.mdb_env)
        }
    }
}

const INODE_SIZE:usize=16;
/// The size of internal patch id. Allocate a buffer of this size when calling e.g. apply.
pub const HASH_SIZE:usize=20; // pub temporaire
const LINE_SIZE:usize=4;
const KEY_SIZE:usize=HASH_SIZE+LINE_SIZE;
const ROOT_INODE:[u8;INODE_SIZE]=[0;INODE_SIZE];
const ROOT_KEY:[u8;KEY_SIZE]=[0;KEY_SIZE];


fn create_new_inode(repo:&Repository,buf:&mut [u8]){
    let curs_tree=Cursor::new(repo.mdb_txn,repo.dbi_tree).unwrap();
    loop {
        for i in 0..INODE_SIZE { buf[i]=rand::random() }
        let mut k = MDB_val{ mv_data:buf.as_ptr() as *const c_void, mv_size:buf.len()as size_t };
        let mut v = MDB_val{ mv_data:ptr::null_mut(), mv_size:0 };
        let e= unsafe { mdb_cursor_get(curs_tree.cursor, &mut k,&mut v,MDB_cursor_op::MDB_SET_RANGE as c_uint) };
        if e==MDB_NOTFOUND { break }
        else if e==0 {
            if (k.mv_size as usize)>=INODE_SIZE {
                if unsafe { memcmp(buf.as_ptr() as *const c_void, k.mv_size as *const c_void, INODE_SIZE as size_t) } != 0 { break }
            } else {
                panic!("Wrong encoding in create_new_inode")
            }
        } else {
            panic!("e!=0 && e!=MDB_NOTFOUND")
        }
    }
}

fn add_inode(repo:&mut Repository, inode:&Option<[u8;INODE_SIZE]>, path:&std::path::Path, is_dir:bool)->Result<(),()>{
    let mut buf:Inode=vec![0;INODE_SIZE];
    let mut components=path.components();
    let mut cs=components.next();
    while let Some(s)=cs { // need to peek at the next element, so no for.
        cs=components.next();
        match s.as_os_str().to_str(){
            Some(ss) => {
                buf.truncate(INODE_SIZE);
                for c in ss.as_bytes() { buf.push(*c as u8) }
                let mut k=MDB_val { mv_data:buf.as_ptr() as *mut c_void, mv_size:buf.len()as size_t };
                let mut v=MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
                let ret= unsafe { mdb_get(repo.mdb_txn,repo.dbi_tree,&mut k,&mut v) };
                if ret==0 {
                    // replace buf with existing inode
                    buf.clear();
                    let _=unsafe { copy_nonoverlapping(v.mv_data as *const c_char,buf.as_mut_ptr() as *mut c_char,v.mv_size as usize) };
                    ()
                } else {
                    let inode = if cs.is_none() && inode.is_some() {
                        inode.unwrap()
                    } else {
                        let mut inode:[u8;INODE_SIZE]=[0;INODE_SIZE];
                        create_new_inode(repo,&mut inode[..]);
                        inode
                    };
                    v.mv_data=inode.as_ptr() as *const c_void;
                    v.mv_size=INODE_SIZE as size_t;
                    //println!("add_inode.adding {:?} !",buf);
                    unsafe { mdb_put(repo.mdb_txn,repo.dbi_tree,&mut k,&mut v,0) };
                    //println!("adding e={}",e);
                    unsafe { mdb_put(repo.mdb_txn,repo.dbi_revtree,&mut v,&mut k,0) };
                    //println!("adding e={}",e);
                    if cs.is_some() || is_dir {
                        k.mv_data="".as_ptr() as *const c_void;
                        k.mv_size=0;
                        unsafe { mdb_put(repo.mdb_txn,repo.dbi_tree,&mut v,&mut k,0) };
                    }
                    // push next inode onto buf.
                    buf.clear();
                    for c in &inode { buf.push(*c) }
                }
            },
            None => {
                return Err(())
            }
        }
    }
    Ok(())
}

/// Adds a file in the repository. Additions need to be recorded in
/// order to produce a patch.
pub fn add_file(repo:&mut Repository, path:&std::path::Path, is_dir:bool)->Result<(),()>{
    //println!("Adding {:?}",path);
    add_inode(repo,&None,path,is_dir)
}



const LINE_HALF_DELETED:c_uchar=16;
const LINE_VISITED:c_uchar=8;
const LINE_ONSTACK:c_uchar=4;
const LINE_SPIT:c_uchar=2;

#[repr(C)]
struct c_line {
    key:*const c_char,
    flags:c_uchar,
    children:*mut*mut c_line,
    children_capacity:size_t,
    children_off:size_t,
    index:c_uint,
    lowlink:c_uint
}

extern "C"{
    // retrieve uses hash tables growing monotonically. For time and
    // memory, we need it in C (or with fast hashtables and no copy of
    // anything) + free without RC.
    fn c_retrieve(txn:*mut MdbTxn,dbi_nodes:MdbDbi,dbi_branches:MdbDbi,
                  branch:*const MDB_val, key:*const c_char) -> *mut c_line;
    fn c_free_line(c_line:*mut c_line);
}

struct Line { c_line:*mut c_line }
impl Drop for Line {
    fn drop(&mut self){
        unsafe {c_free_line(self.c_line)}
    }
}



fn get_current_branch<'a>(repo:&'a Repository)->&'a[u8] {
    unsafe {
        match mdb::get(repo.mdb_txn,repo.dbi_branches,&[0][..]) {
            Ok(b)=>b,
            Err(_)=>DEFAULT_BRANCH.as_bytes()
        }
    }
}

fn retrieve(repo:&Repository,key:&[u8])->Result<Line,()>{
    unsafe {
        let current_branch=get_current_branch(repo);
        let mut cur=MDB_val { mv_data:current_branch.as_ptr() as *const c_void, mv_size:current_branch.len() as size_t };
        let c_line=c_retrieve(repo.mdb_txn,repo.dbi_nodes,repo.dbi_branches,
                              &mut cur,
                              key.as_ptr() as *const c_char);
        if !c_line.is_null() {
            Ok (Line {c_line:c_line})
        } else {Err(())}
    }
}

fn tarjan(line:&mut Line)->usize{
    fn dfs(stack:&mut Vec<*mut c_line>,index:&mut usize, l:*mut c_line){
        unsafe {
            (*l).index = *index as c_uint;
            (*l).lowlink = *index as c_uint;
            (*l).flags |= LINE_ONSTACK | LINE_VISITED;
            stack.push(l);
            *index = *index + 1;
            for i in 0..(*l).children_off {
                let child=*((*l).children.offset(i as isize));
                if (*child).flags & LINE_VISITED == 0 {
                    dfs(stack,index,child);
                    (*l).lowlink=std::cmp::min((*l).lowlink, (*child).lowlink);
                } else {
                    if (*child).flags & LINE_ONSTACK != 0 {
                        (*l).lowlink=std::cmp::min((*l).lowlink, (*child).index)
                    }
                }
            }
            if (*l).index == (*l).lowlink {
                stack.pop();
                while let Some(h)=stack.pop() {
                    if h == l { break }
                }
            }
        }
    }
    let mut stack=vec!();
    let mut index=0;
    dfs(&mut stack, &mut index, line.c_line);
    index-1
}


fn output_file<'a,B>(repo:&'a Repository,buf:&mut B,file:&'a mut Line) where B:LineBuffer<'a> {
    let max_level=tarjan(file);
    let mut counts=vec![0;max_level+1];
    let mut lines=vec![vec!();max_level+1];
    for i in 0..lines.len() { lines[i]=Vec::new() }
    fn fill_lines(counts:&mut Vec<usize>,
                  lines:&mut Vec<Vec<*mut c_line>>,
                  cl:*mut c_line){
        unsafe {
            //println!("fill_lines");
            if (*cl).flags & LINE_SPIT == 0 {
                (*cl).flags |= LINE_SPIT;
                (*counts.get_unchecked_mut((*cl).lowlink as usize)) += 1;
                (*lines.get_unchecked_mut((*cl).lowlink as usize)).push (cl);
                let children:&[*mut c_line]=slice::from_raw_parts((*cl).children, (*cl).children_off as usize);
                for child in children {
                    fill_lines(counts,lines,*child)
                }
            }
        }
    }
    fill_lines(&mut counts, &mut lines, file.c_line);
    // Then add undetected conflicts.
    unsafe  {
        for i in 0..counts.len() {
            if *counts.get_unchecked(i) > 1 {
                for line in lines.get_unchecked(i) {
                    let children:&[*mut c_line]=slice::from_raw_parts((**line).children, (**line).children_off as usize);
                    for child in children {
                        for j in (**line).lowlink+1 .. (**child).lowlink {
                            (*counts.get_unchecked_mut(j as usize)) += 1
                        }}}}}
    }
    fn contents<'a>(repo:&'a Repository,key:&'a[u8]) -> &'a[u8] {
        unsafe {
            let mut k : MDB_val = MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size: key.len() as size_t };
            let mut v : MDB_val = std::mem::zeroed();
            let e=mdb_get(repo.mdb_txn, repo.dbi_contents, &mut k, &mut v);
            if e==0 {
                slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)
            } else {
                &[][..]
            }
        }
    }

    // Finally, output everybody.
    let mut i=0;
    let mut nodes=Vec::new();
    let mut visited=HashSet::new();
    while i<counts.len() {
        //assert!(counts[i]>=1);
        if counts[i]==0 { break }
        else if counts[i] == 1 {
            let key= unsafe { slice::from_raw_parts((*lines[i][0]).key as *const u8, KEY_SIZE as usize) };
            //unsafe { println!("outputting {:?} {}",key,str::from_utf8_unchecked(contents(repo,key))) };
            buf.output_line(&key,contents(repo,key));
            i+=1
        } else {
            fn get_conflict<'a,B>(repo:&'a Repository, counts:&Vec<usize>, l:*const c_line, b:&mut B,
                                  nodes:&mut Vec<&'a[u8]>, visited:&mut HashSet<*const c_line>,
                                  is_first:&mut bool,
                                  next:&mut usize)
            where B:LineBuffer<'a> {
                unsafe {
                    if counts[(*l).lowlink as usize] <= 1 {
                        if ! *is_first {b.output_line(&[][..],b"================================");}else{*is_first=false}
                        for key in nodes {
                            b.output_line(key,contents(repo,key))
                        }
                        *next=(*l).lowlink as usize
                    } else {
                        if !visited.contains(&l) {
                            visited.insert(l);
                            let mut min_order=None;
                            for c in 0..(*l).children_off {
                                let ll=(**((*l).children.offset(c as isize))).lowlink;
                                min_order=Some(match min_order { None=>ll, Some(m)=>std::cmp::min(m,ll) })
                            }
                            match min_order {
                                None=>(),
                                Some(m)=>{
                                    if (*l).flags & LINE_HALF_DELETED != 0 {
                                        for c in 0..(*l).children_off {
                                            let chi=*((*l).children.offset(c as isize));
                                            if (*chi).lowlink==m {
                                                get_conflict(repo,counts,chi,b,nodes,visited,is_first,next)
                                            }
                                        }
                                    }
                                    nodes.push(slice::from_raw_parts((*l).key as *const u8, KEY_SIZE));
                                    for c in 0..(*l).children_off {
                                        let chi=*((*l).children.offset(c as isize));
                                        if (*chi).lowlink==m {
                                            get_conflict(repo,counts,chi,b,nodes,visited,is_first,next)
                                        }
                                    }
                                    let _=nodes.pop();
                                }
                            }
                            let _=visited.remove(&l);
                        }
                    }
                }
            }
            let mut next=0;
            buf.output_line(&[][..],b">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            let mut is_first=true;
            for j in 0..(lines[i].len()) {
                visited.clear();
                nodes.clear();
                get_conflict(repo, &counts,lines[i][j], buf, &mut nodes, &mut visited, &mut is_first, &mut next)
            }
            buf.output_line(&[][..],b"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            i=std::cmp::max(next,i+1);
        }
    }
}



trait LineBuffer<'a> {
    fn output_line(&mut self,&'a[u8],&'a[u8]) -> ();
}

struct Diff<'a> {
    lines_a:Vec<&'a[u8]>,
    contents_a:Vec<&'a[u8]>
}

impl <'a> LineBuffer<'a> for Diff<'a> {
    fn output_line(&mut self,k:&'a[u8],c:&'a[u8]) {
        self.lines_a.push(k);
        self.contents_a.push(c);
    }
}

impl <'a,W> LineBuffer<'a> for W where W:std::io::Write {
    fn output_line(&mut self,_:&'a[u8],c:&'a[u8]) {
        self.write(c).expect("output_line: could not write");
    }
}

/// Gets the external key corresponding to the given key, returning an
/// owned vector. If the key is just a patch id, it returns the
/// corresponding external hash.
fn external_key(repo:&Repository,key:&[u8])->ExternalKey {
    unsafe {
        //println!("internal key:{:?}",&key[0..HASH_SIZE]);
        if memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)==0 {
            //println!("is root key");
            ROOT_KEY.to_vec()
        } else {
            let mut k = MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:HASH_SIZE as size_t };
            let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
            let e = mdb_get(repo.mdb_txn,repo.dbi_external,&mut k, &mut v);
            if e==0 {
                let mut result:Vec<u8>=Vec::with_capacity(v.mv_size as usize+LINE_SIZE);
                let pv=slice::from_raw_parts(v.mv_data as *const c_uchar, v.mv_size as usize);
                for c in pv { result.push(*c) }
                if key.len()==KEY_SIZE { for c in &key[HASH_SIZE..KEY_SIZE] { result.push(*c) } }
                result
            } else {
                println!("internal key:{:?}",key);
                panic!("external key not found !")
            }
        }
    }
}

fn delete_edges<'a>(repo:&'a Repository, edges:&mut Vec<Edge>, key:&'a[u8]){
    // Get external key for "key"
    //println!("delete key: {}",key.to_hex());
    let ext_key=external_key(repo,key);

    // Then collect edges to delete
    let curs=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
    for c in [PARENT_EDGE, PARENT_EDGE|FOLDER_EDGE].iter() {
        unsafe {
            let mut k = MDB_val{ mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v = MDB_val{ mv_data:(c as *const c_uchar) as *const c_void, mv_size:1 };
            let mut e= mdb_cursor_get(curs.cursor, &mut k,&mut v,MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint);
            // take all parent or folder-parent edges:
            //println!("e={}",e);
            while e==0 && v.mv_size>0 && *(v.mv_data as (*mut c_uchar)) == *c {
                if (v.mv_size as usize) < 1+HASH_SIZE+KEY_SIZE {
                    panic!("Wrong encoding in delete_edges")
                }
                // look up the external hash up.
                let pv=slice::from_raw_parts((v.mv_data as *const c_uchar).offset(1), KEY_SIZE as usize);
                let pp=slice::from_raw_parts((v.mv_data as *const c_uchar).offset(1+KEY_SIZE as isize), HASH_SIZE as usize);
                //println!("get key pv");
                let _=external_key(repo,pv);
                //println!("get key pp");
                let _=external_key(repo,pp);

                edges.push(Edge { from:ext_key.clone(), to:external_key(repo,pv), flag:(*c)^DELETED_EDGE, introduced_by:external_key(repo,pp) });
                e= mdb_cursor_get(curs.cursor, &mut k,&mut v,MDB_cursor_op::MDB_NEXT_DUP as c_uint);
            }
        }
    }
}

fn diff(repo:&Repository,line_num:&mut usize, actions:&mut Vec<Change>, a:&mut Line, b:&Path)->Result<(),std::io::Error> {
    fn local_diff(repo:&Repository,actions:&mut Vec<Change>,line_num:&mut usize, lines_a:&[&[u8]], contents_a:&[&[u8]], b:&[&[u8]]) {
        let mut opt=vec![vec!();contents_a.len()+1];
        for i in 0..opt.len() { opt[i]=vec![0;b.len()+1] }
        // opt
        for i in (0..contents_a.len()).rev() {
            for j in (0..b.len()).rev() {
                opt[i][j]=
                    if contents_a[i]==b[j] { opt[i+1][j+1]+1 } else { std::cmp::max(opt[i+1][j], opt[i][j+1]) }
            }
        }
        let mut i=1;
        let mut j=0;
        fn add_lines(repo:&Repository,actions:&mut Vec<Change>, line_num:&mut usize,
                     up_context:&[u8],down_context:&[&[u8]],lines:&[&[u8]]){
            actions.push(
                Change::NewNodes {
                    up_context:vec!(external_key(repo,up_context)),
                    down_context:down_context.iter().map(|x|{external_key(repo,x)}).collect(),
                    line_num: *line_num,
                    flag:0,
                    nodes:lines.iter().map(|x|{x.to_vec()}).collect()
                });
            *line_num += lines.len()
        }
        fn delete_lines(repo:&Repository,actions:&mut Vec<Change>, lines:&[&[u8]]){
            let mut edges=Vec::with_capacity(lines.len());
            for l in lines {
                delete_edges(repo,&mut edges,l)
            }
            actions.push(Change::Edges(edges))
        }
        let mut oi=None;
        let mut oj=None;
        while i<contents_a.len() && j<b.len() {
            if contents_a[i]==b[j] {
                if let Some(i0)=oi {
                    delete_lines(repo,actions, &lines_a[i0..i]);
                    oi=None
                } else if let Some(j0)=oj {
                    add_lines(repo,actions, line_num,
                              lines_a[i-1], // up context
                              &lines_a[i..i+1], // down context
                              &b[j0..j]);
                    oj=None
                }
                i+=1; j+=1;
            } else {
                if opt[i+1][j] >= opt[i][j+1] {
                    if let Some(j0)=oj {
                        add_lines(repo,actions, line_num,
                                  lines_a[i-1], // up context
                                  &lines_a[i..i+1], // down context
                                  &b[j0..j]);
                        oj=None
                    }
                    if oi.is_none() { oi=Some(i) }
                    i+=1
                } else {
                    if let Some(i0)=oi {
                        delete_lines(repo,actions, &lines_a[i0..i]);
                        oi=None
                    }
                    if oj.is_none() { oj=Some(j) }
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
            delete_lines(repo,actions, &lines_a[i..lines_a.len()])
        } else if j < b.len() {
            if let Some(i0)=oi {
                delete_lines(repo,actions, &lines_a[i0..i]);
            }
            add_lines(repo,actions, line_num, lines_a[i-1], &[][..], &b[j..b.len()])
        }
    }

    let mut buf_b=Vec::new();
    let mut lines_b=Vec::new();
    let err={
        let f = std::fs::File::open(b);
        let mut f = std::io::BufReader::new(f.unwrap());
        let err=f.read_to_end(&mut buf_b);
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
            let mut d = Diff { lines_a:Vec::new(), contents_a:Vec::new() };
            output_file(repo,&mut d,a);
            let mut i=0;
            while i+1<d.contents_a.len() && i<lines_b.len() && d.contents_a[i+1]==lines_b[i] { i+=1; }
            local_diff(repo,actions, line_num,
                       &d.lines_a[i..],
                       &d.contents_a[i..],
                       &lines_b[i..]);
            Ok(())
        },
        Err(e)=>Err(e)
    }
}




const PSEUDO_EDGE:u8=1;
const FOLDER_EDGE:u8=2;
const PARENT_EDGE:u8=4;
const DELETED_EDGE:u8=8;
pub type Inode=Vec<u8>;

/// Records,i.e. produce a patch and a HashMap mapping line numbers to inodes.
pub fn record<'a>(repo:&'a mut Repository,working_copy:&std::path::Path)->Result<(Vec<Change>,HashMap<LocalKey,Inode>),Error>{
    fn dfs(repo:&Repository, actions:&mut Vec<Change>,
           curs_tree:&mut Cursor,
           line_num:&mut usize,updatables:&mut HashMap<Vec<u8>,Vec<u8>>,
           parent_inode:Option<&[u8]>,
           parent_node:Option<&[u8]>,
           current_inode:&[u8],
           realpath:&mut std::path::PathBuf, basename:&[u8]) {

        if parent_inode.is_some() { realpath.push(str::from_utf8(&basename).unwrap()) }

        let mut k = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let root_key=&ROOT_KEY[..];
        let mut l2=[0;LINE_SIZE];
        let current_node=
            if parent_inode.is_some() {
                k.mv_data=current_inode.as_ptr() as *const c_void;
                k.mv_size=INODE_SIZE as size_t;
                let e = unsafe { mdb_get(repo.mdb_txn,repo.dbi_inodes,&mut k, &mut v) };
                if e==0 { // This inode already has a corresponding node
                    let current_node=unsafe { slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize) };
                    //println!("Existing node: {}",current_node.to_hex());
                    if current_node[0]==1 {
                        // file moved
                        println!("file move not implemented in record");
                        unimplemented!()
                    } else if current_node[0]==2 {
                        // file deleted. delete recursively
                        println!("file deletions not implemented in record");
                        unimplemented!()
                    } else if current_node[0]==0 {
                        let ret=retrieve(repo,&current_node[3..]);
                        diff(repo,line_num,actions, &mut ret.unwrap(), realpath.as_path()).unwrap()
                    } else {
                        panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                    };
                    Some(current_node)
                } else {
                    // File addition, create appropriate Newnodes.
                    match metadata(&realpath) {
                        Ok(attr) => {
                            //println!("file addition, realpath={:?}", realpath);
                            let permissions=attr.permissions().mode() as usize;
                            let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                            let mut nodes=Vec::new();
                            let mut lnum= *line_num + 1;
                            for i in 0..(LINE_SIZE-1) { l2[i]=(lnum & 0xff) as u8; lnum=lnum>>8 }

                            let mut name=Vec::with_capacity(basename.len()+2);
                            let int_attr=permissions | is_dir;
                            name.push(((int_attr >> 8) & 0xff) as u8);
                            name.push((int_attr & 0xff) as u8);
                            for c in basename { name.push(*c) }
                            actions.push(
                                Change::NewNodes { up_context: vec!(parent_node.unwrap().to_vec()),
                                                   line_num: *line_num,
                                                   down_context: vec!(),
                                                   nodes: vec!(name,vec!()),
                                                   flag:FOLDER_EDGE }
                                );
                            *line_num += 2;

                            // Reading the file
                            if is_dir==0 {
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
                                updatables.insert(l2.to_vec(),current_inode.to_vec());
                                let len=nodes.len();
                                actions.push(
                                    Change::NewNodes { up_context:vec!(l2.to_vec()),
                                                       line_num: *line_num,
                                                       down_context: vec!(),
                                                       nodes: nodes,
                                                       flag:0 }
                                    );
                                *line_num+=len;
                                Some(&l2[..])
                            } else {
                                None
                            }
                        },
                        Err(_)=>{
                            panic!("error adding a file (metadata failed)")
                        }
                    }
                }
            } else {
                Some(root_key)
            };


        match current_node {
            None => (), // we just added a file
            Some(current_node)=>{
                k.mv_data=current_inode.as_ptr() as *const c_void;
                k.mv_size=INODE_SIZE as size_t;

                let mut children=Vec::new();

                let mut e= unsafe { mdb_cursor_get(curs_tree.cursor, &mut k,&mut v,MDB_cursor_op::MDB_SET_RANGE as c_uint) };
                while e==0 && unsafe { memcmp(k.mv_data as *const c_void, current_inode.as_ptr() as *const c_void, INODE_SIZE as size_t) } == 0 {
                    if k.mv_size>INODE_SIZE as size_t {
                        unsafe {
                            children.push((slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize),
                                           slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)));
                            e=mdb_cursor_get(curs_tree.cursor,&mut k,&mut v,MDB_cursor_op::MDB_NEXT as c_uint);
                        }
                    }
                }
                for chi in children {
                    let (ks,vs)= chi;
                    let (_,next_basename)=ks.split_at(INODE_SIZE);
                    //println!("next_basename={:?}",String::from_utf8_lossy(next_basename));
                    let _=
                        dfs(repo, actions, curs_tree, line_num,updatables,
                            Some(current_inode), // parent_inode
                            Some(current_node), // parent_node
                            vs,// current_inode
                            realpath,
                            next_basename);
                }
                if parent_inode.is_some() { let _=realpath.pop(); }
            }
        }
    };
    let mut actions:Vec<Change>=Vec::new();
    let mut line_num=1;
    let mut updatables:HashMap<Vec<u8>,Vec<u8>>=HashMap::new();
    let mut realpath=PathBuf::from(working_copy);
    let mut curs_tree=try!(Cursor::new(repo.mdb_txn,repo.dbi_tree));
    dfs(repo,&mut actions,&mut curs_tree, &mut line_num,&mut updatables,
        None,None,&ROOT_INODE[..],&mut realpath,
        &[][..]);
    //println!("record done");
    Ok((actions,updatables))
}


fn internal_hash<'a>(txn:*mut MdbTxn,dbi:MdbDbi,key:&'a [u8])->&'a [u8] {
    unsafe {
        if memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)==0 {
            slice::from_raw_parts(ROOT_KEY.as_ptr(), ROOT_KEY.len())
        } else {
            let mut k = MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v:MDB_val =std::mem::zeroed();
            let e = mdb_get(txn,dbi,&mut k, &mut v);
            if e==0 {
                slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)
            } else {
                println!("external key:{}",key.to_hex());
                panic!("internal key not found !")
            }
        }
    }
}

fn unsafe_apply(repo:&mut Repository,changes:&[Change], internal_patch_id:&[u8]){
    let curs=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
    let mut uu:MDB_val= unsafe {std::mem::zeroed() };
    let mut vv:MDB_val= unsafe {std::mem::zeroed() };
    for ch in changes {
        match *ch {
            Change::Edges(ref edges) =>
                for e in edges {
                    //println!("edge");
                    // First remove the deleted version of the edge
                    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];

                    pu[0]=e.flag ^ DELETED_EDGE ^ PARENT_EDGE;
                    pv[0]=e.flag ^ DELETED_EDGE;
                    let _=unsafe {
                        let u=internal_hash(repo.mdb_txn,repo.dbi_internal,&e.from[0..(e.from.len()-LINE_SIZE)]);
                        copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                            pu.as_mut_ptr().offset(1+HASH_SIZE as isize), LINE_SIZE);
                        copy_nonoverlapping(u.as_ptr(),pu.as_mut_ptr().offset(1), HASH_SIZE)
                    };
                    let _=unsafe {
                        let v=internal_hash(repo.mdb_txn,repo.dbi_internal,&e.to[0..(e.to.len()-LINE_SIZE)]);
                        copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                            pv.as_mut_ptr().offset(1+HASH_SIZE as isize), LINE_SIZE);
                        copy_nonoverlapping(v.as_ptr(),pv.as_mut_ptr().offset(1), HASH_SIZE)
                    };
                    //println!("internal: {}\n          {}",pu.to_hex(),pv.to_hex());
                    let _=unsafe {
                        let p=internal_hash(repo.mdb_txn,repo.dbi_internal,&e.introduced_by[..]);
                        copy_nonoverlapping(p.as_ptr(),
                             pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                             HASH_SIZE);
                        copy_nonoverlapping(p.as_ptr(),
                             pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                             HASH_SIZE)
                    };
                    let _=unsafe {
                        uu.mv_data=(pu.as_ptr().offset(1)) as *mut c_void;
                        uu.mv_size=KEY_SIZE as size_t;
                        vv.mv_data=(pv.as_ptr()) as *mut c_void;
                        vv.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        let e=mdb_cursor_get(curs.cursor,&mut uu,&mut vv,MDB_cursor_op::MDB_GET_BOTH as c_uint);
                        if e==0 { mdb_cursor_del(curs.cursor,0) } else {e};
                        uu.mv_data=(pv.as_ptr().offset(1)) as *mut c_void;
                        uu.mv_size=KEY_SIZE as size_t;
                        vv.mv_data=(pu.as_ptr()) as *mut c_void;
                        vv.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        let e=mdb_cursor_get(curs.cursor,&mut uu,&mut vv,MDB_cursor_op::MDB_GET_BOTH as c_uint);
                        if e==0 { mdb_cursor_del(curs.cursor,0) } else {e};
                    };
                    // Then add the new edges
                    unsafe {
                        pu[0]=e.flag^PARENT_EDGE;
                        pv[0]=e.flag;
                        uu.mv_data=(pu.as_ptr().offset(1)) as *mut c_void;
                        uu.mv_size=KEY_SIZE as size_t;
                        vv.mv_data=(pv.as_ptr()) as *mut c_void;
                        vv.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut uu,&mut vv,MDB_NODUPDATA);
                        uu.mv_data=(pv.as_ptr().offset(1)) as *mut c_void;
                        uu.mv_size=KEY_SIZE as size_t;
                        vv.mv_data=(pu.as_ptr()) as *mut c_void;
                        vv.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut uu,&mut vv,MDB_NODUPDATA);
                    }
                },
            Change::NewNodes { ref up_context,ref down_context,ref line_num,ref flag,ref nodes } => {
                //println!("newnodes");
                let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                let mut lnum0= *line_num;
                for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                let _= unsafe {
                    copy_nonoverlapping(internal_patch_id.as_ptr(),
                         pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                         HASH_SIZE);
                    copy_nonoverlapping(internal_patch_id.as_ptr(),
                         pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                         HASH_SIZE);
                    copy_nonoverlapping(internal_patch_id.as_ptr(),
                         pv.as_mut_ptr().offset(1),
                         HASH_SIZE)
                };
                //println!("pu={}",pu.to_hex());
                //println!("ipi={}",internal_patch_id.to_hex());
                for c in up_context {
                    unsafe {
                        let mut v0 = MDB_val { mv_data:(pu.as_ptr().offset(1)) as *const c_void,
                                               mv_size:KEY_SIZE as size_t };
                        let mut v1 = MDB_val { mv_data:pv.as_ptr() as *const c_void,
                                               mv_size:(1+KEY_SIZE+HASH_SIZE) as size_t };
                        {
                            //println!("c.len={}, LINE_SIZE={}",c.len(), LINE_SIZE);
                            let u= if c.len()>LINE_SIZE {
                                internal_hash(repo.mdb_txn,repo.dbi_internal,&c[0..(c.len()-LINE_SIZE)])
                            } else {
                                internal_patch_id
                            };
                            //println!("u={}",u.to_hex());
                            copy_nonoverlapping(u.as_ptr() as *const c_char,
                                 pu.as_ptr().offset(1) as *mut c_char,
                                 HASH_SIZE);
                        }
                        copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                            pu.as_mut_ptr().offset((1+HASH_SIZE) as isize),
                                            LINE_SIZE);
                        pu[0]= (*flag) ^ PARENT_EDGE;
                        pv[0]= *flag;
                        mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut v0, &mut v1, MDB_NODUPDATA);
                        v0.mv_data=pv.as_ptr().offset(1) as *const c_void;
                        v0.mv_size=KEY_SIZE as size_t;
                        v1.mv_data=pu.as_ptr() as *const c_void;
                        v1.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        //println!("put (up): {}",pu.to_hex());
                        mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut v0, &mut v1, MDB_NODUPDATA);
                        //println!("put e={}",e);
                    }
                }
                //////////////
                unsafe {
                    copy_nonoverlapping(internal_patch_id.as_ptr() as *const c_char,
                         pu.as_ptr().offset(1) as *mut c_char,
                         HASH_SIZE);
                }

                let mut lnum= *line_num + 1;
                let mut v0:MDB_val = unsafe { std::mem::zeroed () };
                let mut v1:MDB_val = unsafe { std::mem::zeroed () };
                unsafe {
                    v0.mv_data=pv.as_ptr().offset(1) as *const c_void;
                    v0.mv_size=KEY_SIZE as size_t;
                    v1.mv_data=nodes[0].as_ptr() as *const c_void;
                    v1.mv_size=nodes[0].len() as size_t;
                    mdb_put(repo.mdb_txn,repo.dbi_contents,&mut v0, &mut v1, 0);
                }
                for n in &nodes[1..] {
                    let mut lnum0=lnum-1;
                    for i in 0..LINE_SIZE { pu[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                    lnum0=lnum;
                    for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                    pu[0]= (*flag)^PARENT_EDGE;
                    pv[0]= *flag;
                    unsafe {
                        v0.mv_data=pu.as_ptr().offset(1) as *const c_void;
                        v0.mv_size=KEY_SIZE as size_t;
                        v1.mv_data=pv.as_ptr() as *const c_void;
                        v1.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut v0, &mut v1, MDB_NODUPDATA);
                        v0.mv_data=pv.as_ptr().offset(1) as *const c_void;
                        v0.mv_size=KEY_SIZE as size_t;
                        v1.mv_data=pu.as_ptr() as *const c_void;
                        v1.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        //println!("lnum={}",lnum);
                        //println!("adding node {} -> {}", pu.to_hex(), pv.to_hex());
                        mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut v0, &mut v1, MDB_NODUPDATA);
                        v1.mv_data=n.as_ptr() as *const c_void;
                        v1.mv_size=n.len() as size_t;
                        v0.mv_data=pv.as_ptr().offset(1) as *const c_void;
                        v0.mv_size=KEY_SIZE as size_t;
                        mdb_put(repo.mdb_txn,repo.dbi_contents,&mut v0, &mut v1, 0);
                    }
                    lnum = lnum+1;
                }
                // In this last part, u is that target (downcontext), and v is the last new node.
                for c in down_context {
                    unsafe {
                        {
                            let u= if c.len()>LINE_SIZE {
                                internal_hash(repo.mdb_txn,repo.dbi_internal,&c[0..(c.len()-LINE_SIZE)])
                            } else {
                                internal_patch_id
                            };
                            copy_nonoverlapping(u.as_ptr(), pu.as_mut_ptr().offset(1), HASH_SIZE)
                        }
                        //println!("c.len={}",c.len());
                        copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize) as *const c_char,
                                            pu.as_ptr().offset((1+HASH_SIZE) as isize) as *mut c_char,
                                            LINE_SIZE);
                        pu[0]= *flag;
                        pv[0]= (*flag) ^ PARENT_EDGE;

                        let mut v0 = MDB_val { mv_data:(pu.as_ptr().offset(1)) as *const c_void,
                                               mv_size:KEY_SIZE as size_t };
                        let mut v1 = MDB_val { mv_data:pv.as_ptr() as *const c_void,
                                               mv_size:(1+KEY_SIZE+HASH_SIZE) as size_t };
                        let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut v0, &mut v1, MDB_NODUPDATA);
                        v0.mv_data=pv.as_ptr().offset(1) as *const c_void;
                        v0.mv_size=KEY_SIZE as size_t;
                        v1.mv_data=pu.as_ptr() as *const c_void;
                        v1.mv_size=(1+KEY_SIZE+HASH_SIZE) as size_t;
                        let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut v0, &mut v1, MDB_NODUPDATA);
                    }
                }
            }

        }
    }
}

/// Create a new internal patch id, register it in the "external" and
/// "internal" bases, and write the result in its second argument
/// ("result").
pub fn new_internal(repo:&mut Repository,result:&mut[u8]) {
    let curs=Cursor::new(repo.mdb_txn,repo.dbi_external).unwrap();
    let root_key=&ROOT_KEY[0..HASH_SIZE];
    let last=
        unsafe {
            let mut k:MDB_val=std::mem::zeroed();
            let mut v:MDB_val=std::mem::zeroed();
            let e=mdb_cursor_get(curs.cursor,&mut k,&mut v, MDB_cursor_op::MDB_LAST as c_uint);
            if e==0 && (k.mv_size as usize)>=HASH_SIZE {
                slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize)
            } else {
                root_key
            }
        };
    fn create_new(r:&mut [u8],last:&[u8],i:usize){
        if i>0 {
            if last[i]==0xff { r[i]=0; create_new(r,last,i-1) }
            else { r[i]=last[i]+1 }
        }
    }
    create_new(result,last,last.len()-1);
}
pub fn register_hash(repo:&mut Repository,internal:&[u8],external:&[u8]){
    unsafe {
        //println!("apply:registering external {}",external.to_hex());
        //println!("apply:registering external {}",result.to_hex());
        let mut exter = MDB_val { mv_data:external.as_ptr() as *const c_void, mv_size:external.len() as size_t };
        let mut inter = MDB_val { mv_data:internal.as_ptr() as *const c_void, mv_size:HASH_SIZE as size_t};
        let _=mdb_put(repo.mdb_txn,repo.dbi_external,&mut inter, &mut exter, MDB_NODUPDATA);
        exter.mv_data=external.as_ptr() as *const c_void;
        exter.mv_size=external.len() as size_t;
        inter.mv_data=internal.as_ptr() as *const c_void;
        inter.mv_size=internal.len() as size_t;
        let _=mdb_put(repo.mdb_txn,repo.dbi_internal,&mut exter, &mut inter, MDB_NODUPDATA);
    }
}

/// The name of the default branch, "main".
pub const DEFAULT_BRANCH:&'static str="main";

/// Applies a set of changes to a repository. The changes need to come from a patch.
pub fn apply(repo:&mut Repository, patch:&patch::Patch, internal:&[u8]) {
    unsafe_apply(repo,&patch.changes[..],internal);
    let mut k = {
        let c:[u8;1]=[0];
        MDB_val { mv_data:c.as_ptr() as *const c_void, mv_size:1 }
    };
    {
        let current=get_current_branch(repo);
        let mut v=MDB_val { mv_data:current.as_ptr()as *const c_void,mv_size:current.len() as size_t};
        k.mv_data=internal.as_ptr() as *const c_void;
        k.mv_size=internal.len() as size_t;
        let _= unsafe { mdb_put(repo.mdb_txn,repo.dbi_branches, &mut v, &mut k, MDB_NODUPDATA) };
    }
    for ch in patch.changes.iter() {
        match *ch {
            Change::Edges(ref edges) =>{
                for e in edges {
                    let hu=internal_hash(repo.mdb_txn,repo.dbi_internal,&e.from[0..(e.from.len()-LINE_SIZE)]);
                    let hv=internal_hash(repo.mdb_txn,repo.dbi_internal,&e.to[0..(e.to.len()-LINE_SIZE)]);
                    let mut u:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    let mut v:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    unsafe {
                        copy_nonoverlapping(hu.as_ptr(),u.as_mut_ptr(),HASH_SIZE);
                        copy_nonoverlapping(hv.as_ptr(),v.as_mut_ptr(),HASH_SIZE);
                        copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                            u.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                        copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                            v.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                    }
                    if e.flag&DELETED_EDGE!=0 {
                        if e.flag&PARENT_EDGE!=0 {
                            if e.flag&FOLDER_EDGE!=0 {
                                connect_zombie_folders(repo,&v,&u,&internal)
                            } else {
                                connect_down(repo,&v,&u,&internal)
                            }
                        } else {
                            if e.flag&FOLDER_EDGE!=0 {
                                connect_zombie_folders(repo,&u,&v,&internal)
                            } else {
                                connect_down(repo,&u,&v,&internal)
                            }
                        }
                    } else {
                        if e.flag&PARENT_EDGE!=0 {
                            connect_up(repo,&v,&u,&internal)
                        } else {
                            connect_up(repo,&u,&v,&internal)
                        }
                    }
                }
            },
            Change::NewNodes { ref up_context,ref down_context,ref line_num,flag:_,ref nodes } => {
                let mut pu:[u8;KEY_SIZE]=[0;KEY_SIZE];
                let mut pv:[u8;KEY_SIZE]=[0;KEY_SIZE];
                let mut lnum0= *line_num;
                for i in 0..LINE_SIZE { pv[HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                let _= unsafe {
                    copy_nonoverlapping(internal.as_ptr(),
                                        pv.as_mut_ptr(),
                                        HASH_SIZE)
                };
                for c in up_context {
                    unsafe {
                        let u= if c.len()>LINE_SIZE {
                            internal_hash(repo.mdb_txn,repo.dbi_internal,&c[0..(c.len()-LINE_SIZE)])
                        } else {
                            internal as &[u8]
                        };
                        copy_nonoverlapping(u.as_ptr(), pu.as_mut_ptr(), HASH_SIZE);
                        connect_up(repo,&pu,&pv,&internal)
                    }
                }
                lnum0= (*line_num)+nodes.len()-1;
                for i in 0..LINE_SIZE { pv[HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                for c in down_context {
                    unsafe {
                        let u= if c.len()>LINE_SIZE {
                            internal_hash(repo.mdb_txn,repo.dbi_internal,&c[0..(c.len()-LINE_SIZE)])
                        } else {
                            internal as &[u8]
                        };
                        copy_nonoverlapping(u.as_ptr(), pv.as_mut_ptr(), HASH_SIZE);
                        connect_down(repo,&pv,&pu,&internal)
                    }
                }
            }
        }
    }
    for ref dep in patch.dependencies.iter() {
        let dep_internal=internal_hash(repo.mdb_txn,repo.dbi_internal,&dep[..]);
        unsafe {
            mdb::put(repo.mdb_txn,repo.dbi_revdep,dep_internal,internal,0).unwrap()
        }
    }

}


pub fn dependencies(changes:&[Change])->Vec<patch::ExternalHash> {
    let mut deps=Vec::new();
    for ch in changes {
        match *ch {
            Change::NewNodes { ref up_context,ref down_context, line_num:_,flag:_,nodes:_ } => {
                for c in up_context.iter().chain(down_context.iter()) {
                    deps.push(c[0..c.len()-LINE_SIZE].to_vec())
                }
            },
            Change::Edges(ref edges) =>{
                for e in edges {
                    deps.push(e.from[0..e.from.len()-LINE_SIZE].to_vec());
                    deps.push(e.to[0..e.to.len()-LINE_SIZE].to_vec());
                    deps.push(e.introduced_by.clone())
                }
            }
        }
    }
    deps
}


/// This function is used to add the pseudo-edges collected in
/// connect_up and connect_down. This is because these functions
/// cannot directly modify the database while iterating on cursors
fn reconnect(repo:&mut Repository,pu:&mut [u8], buf:&[u8]){
    let mut k: MDB_val=unsafe { std::mem::zeroed() };
    let mut v: MDB_val=unsafe { std::mem::zeroed() };
    let mut i=0;
    while i<buf.len(){
        pu[0]=buf[i] ^ PARENT_EDGE;
        unsafe {
            k.mv_data= pu.as_ptr().offset(1) as *const c_void;
            k.mv_size= KEY_SIZE as size_t;
            v.mv_data= buf.as_ptr().offset(i as isize) as *const c_void;
            v.mv_size= (1+KEY_SIZE+HASH_SIZE) as size_t;
            let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut k,&mut v,MDB_NODUPDATA);

            k.mv_data= buf.as_ptr().offset((i+1) as isize) as *const c_void;
            k.mv_size= KEY_SIZE as size_t;
            v.mv_data= pu.as_ptr() as *const c_void;
            v.mv_size= (1+KEY_SIZE+HASH_SIZE) as size_t;
            let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut k,&mut v,MDB_NODUPDATA);
        }
        i+=1+KEY_SIZE+HASH_SIZE
    }
}

/// When an edge (a,b) is added, add a pseudo-edge from the first
/// alive ancestors of a to b.
fn connect_up(repo:&mut Repository, a0:&[u8], b:&[u8],internal_patch_id:&[u8]) {
    fn connect<'a>(visited:&mut HashSet<&'a[u8]>, repo:&Repository, a:&'a[u8], internal_patch_id:&'a[u8], buf:&mut Vec<u8>, folder_buf:&mut Vec<u8>) {
        if !visited.contains(a) {
            visited.insert(a);
            // Follow parent edges.
            let cursor=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
            let flag= PARENT_EDGE|DELETED_EDGE;
            let mut k= MDB_val { mv_data:a.as_ptr() as *const c_void, mv_size:a.len() as size_t };
            let mut v= MDB_val { mv_data:[flag].as_ptr() as *const c_void, mv_size:1 };
            let mut e= unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
            while e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == flag }) {
                let a1= unsafe {slice::from_raw_parts( (v.mv_data as *const u8).offset(1), KEY_SIZE ) };
                connect(visited,repo,a1,internal_patch_id,buf,folder_buf);
                e = unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_NEXT_DUP as c_uint) };
            }
            // Then add zombie folder edges
            let flag=[PARENT_EDGE|DELETED_EDGE|FOLDER_EDGE];
            let mut k= MDB_val { mv_data:a.as_ptr() as *const c_void, mv_size:a.len() as size_t };
            let mut v= MDB_val { mv_data:flag.as_ptr() as *const c_void, mv_size:1 };
            let mut e= unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
            while e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == flag[0] }) {
                let a1= unsafe {slice::from_raw_parts( (v.mv_data as *const u8).offset(1), KEY_SIZE ) };

                folder_buf.push(PSEUDO_EDGE|PARENT_EDGE|FOLDER_EDGE);
                for i in 0..KEY_SIZE { folder_buf.push(a[i]) }
                for i in 0..HASH_SIZE { folder_buf.push(internal_patch_id[i]) }

                folder_buf.push(PSEUDO_EDGE|FOLDER_EDGE);
                for i in 0..KEY_SIZE { folder_buf.push(a1[i]) }
                for i in 0..HASH_SIZE { folder_buf.push(internal_patch_id[i]) }

                e = unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_NEXT_DUP as c_uint) };
            }
            // Test for life of the current node
            let flag=[PARENT_EDGE];
            let mut k= MDB_val { mv_data:a.as_ptr() as *const c_void, mv_size:a.len() as size_t };
            let mut v= MDB_val { mv_data:flag.as_ptr() as *const c_void, mv_size:1 };
            let e = unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
            if e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == PARENT_EDGE }) {
                // If there needs to be a pseudo-edge here, put the
                // edge in a buffer, as it could disturb iterators
                // above here in the DFS.
                buf.push(PSEUDO_EDGE|PARENT_EDGE);
                for i in 0..KEY_SIZE { buf.push(a[i]) }
                for i in 0..HASH_SIZE { buf.push(internal_patch_id[i]) }
            }
        }
    }
    let mut visited=HashSet::new();
    let mut buf=Vec::new();
    let mut folder_buf=Vec::new();
    connect(&mut visited, repo,a0,internal_patch_id,&mut buf,&mut folder_buf);
    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
    unsafe {
        copy_nonoverlapping(b.as_ptr(), pu.as_mut_ptr().offset(1), KEY_SIZE);
        copy_nonoverlapping(internal_patch_id.as_ptr(), pu.as_mut_ptr().offset((1+KEY_SIZE) as isize), HASH_SIZE)
    }
    reconnect(repo,&mut pu, &buf[..]);

    let mut i=0;
    while i<folder_buf.len(){
        unsafe {
            mdb::put(repo.mdb_txn,repo.dbi_nodes,
                     &folder_buf[(i+1)..(i+1+KEY_SIZE)],
                     &folder_buf[(i+1+KEY_SIZE+HASH_SIZE)..(i+2*(1+KEY_SIZE+HASH_SIZE))],0).unwrap();
            mdb::put(repo.mdb_txn,repo.dbi_nodes,
                     &folder_buf[(i+1+KEY_SIZE+HASH_SIZE+1)..(i+1+KEY_SIZE+HASH_SIZE+KEY_SIZE)],
                     &folder_buf[i..(i+1+KEY_SIZE+HASH_SIZE)],0).unwrap();
        }
        i+=2*(1+KEY_SIZE+HASH_SIZE)
    }


}

/// When an edge (a,b) is deleted, add a pseudo-edge from a to the earliest alive descendants of b.
fn connect_down(repo:&mut Repository, a:&[u8], b0:&[u8],internal_patch_id:&[u8]) {
    fn connect<'a>(visited:&mut HashSet<&'a[u8]>, repo:&Repository, b:&'a[u8], internal_patch_id:&'a[u8], buf:&mut Vec<u8>) {
        if !visited.contains(b) {
            visited.insert(b);
            let cursor=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
            let flag= 0;
            let mut k= MDB_val { mv_data:b.as_ptr() as *const c_void, mv_size:b.len() as size_t };
            let mut v= MDB_val { mv_data:[flag].as_ptr() as *const c_void, mv_size:1 };
            let mut e= unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
            while e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == flag }) {
                let b1= unsafe {slice::from_raw_parts( (v.mv_data as *const u8).offset(1), KEY_SIZE ) };
                connect(visited,repo,b1,internal_patch_id,buf);
                e = unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_NEXT_DUP as c_uint) };
            }
            let flag=[PARENT_EDGE];
            let mut k= MDB_val { mv_data:b.as_ptr() as *const c_void, mv_size:b.len() as size_t };
            let mut v= MDB_val { mv_data:flag.as_ptr() as *const c_void, mv_size:1 };
            e= unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
            if e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == PARENT_EDGE }) {
                // If there needs to be a pseudo-edge here
                buf.push(PSEUDO_EDGE);
                for i in 0..KEY_SIZE { buf.push(b[i]) }
                for i in 0..HASH_SIZE { buf.push(internal_patch_id[i]) }
            }
        }
    }
    let mut visited=HashSet::new();
    let mut buf=Vec::new();
    connect(&mut visited, repo,b0,internal_patch_id,&mut buf);
    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
    unsafe {
        copy_nonoverlapping(a.as_ptr(), pu.as_mut_ptr().offset(1), KEY_SIZE);
        copy_nonoverlapping(internal_patch_id.as_ptr(), pu.as_mut_ptr().offset((1+KEY_SIZE) as isize), HASH_SIZE)
    }
    reconnect(repo,&mut pu,&buf[..]);
}

/// Adds a pseudo-edge between a and b if b has alive folder descendants.
/// This is meant to detect conflicts where Alice deletes a folder F while Bob adds a file in F.
fn connect_zombie_folders(repo:&mut Repository, a:&[u8], b:&[u8],internal_patch_id:&[u8]) {
    fn has_alive_descendants<'a>(repo:&'a Repository,b:&'a [u8],visited:&mut HashSet<&'a[u8]>)->bool {
        if !visited.contains(b) {
            visited.insert(b);
            let cursor=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
            let flag= FOLDER_EDGE | DELETED_EDGE;
            let mut k= MDB_val { mv_data:b.as_ptr() as *const c_void, mv_size:b.len() as size_t };
            let mut v= MDB_val { mv_data:[flag].as_ptr() as *const c_void, mv_size:1 };
            let mut has_alive_desc=false;
            let mut e= unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
            while e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == flag }) && !has_alive_desc {
                let b1= unsafe {slice::from_raw_parts( (v.mv_data as *const u8).offset(1), KEY_SIZE ) };
                has_alive_desc = has_alive_descendants(repo,b1,visited);
                e = unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_NEXT_DUP as c_uint) };
            }
            has_alive_desc || {
                let flag=[PARENT_EDGE|FOLDER_EDGE];
                let mut k= MDB_val { mv_data:b.as_ptr() as *const c_void, mv_size:b.len() as size_t };
                let mut v= MDB_val { mv_data:flag.as_ptr() as *const c_void, mv_size:1 };
                let e= unsafe { mdb_cursor_get(cursor.cursor, &mut k, &mut v, MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint) };
                (e==0 && v.mv_size>=1 && (unsafe { *(v.mv_data as *const u8) == flag[0] }))
            }
        } else { false }
    }
    let mut visited=HashSet::new();
    if has_alive_descendants(repo,b,&mut visited) {
        let mut pu=[0;1+HASH_SIZE+KEY_SIZE];
        let mut pv=[0;1+HASH_SIZE+KEY_SIZE];
        unsafe {
            copy_nonoverlapping(a.as_ptr(), pu.as_mut_ptr().offset(1), KEY_SIZE);
            copy_nonoverlapping(internal_patch_id.as_ptr(), pu.as_mut_ptr().offset((1+KEY_SIZE) as isize), HASH_SIZE);
            pu[0]=FOLDER_EDGE|PARENT_EDGE;
            copy_nonoverlapping(b.as_ptr(), pv.as_mut_ptr().offset(1), KEY_SIZE);
            copy_nonoverlapping(internal_patch_id.as_ptr(), pv.as_mut_ptr().offset((1+KEY_SIZE) as isize), HASH_SIZE);
            pv[0]=FOLDER_EDGE;
            let mut k: MDB_val=MDB_val { mv_data:pu.as_ptr().offset(1) as *const c_void, mv_size:KEY_SIZE as size_t };
            let mut v: MDB_val=MDB_val { mv_data:pv.as_ptr() as *const c_void, mv_size:pv.len() as size_t };
            let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut k,&mut v,MDB_NODUPDATA);
            k.mv_data= pv.as_ptr().offset(1) as *const c_void;
            k.mv_size= KEY_SIZE as size_t;
            v.mv_data= pu.as_ptr() as *const c_void;
            v.mv_size= (1+KEY_SIZE+HASH_SIZE) as size_t;
            let _=mdb_put(repo.mdb_txn,repo.dbi_nodes,&mut k,&mut v,MDB_NODUPDATA);
        }
    }
}



pub fn sync_file_additions(repo:&mut Repository, changes:&[Change], updates:&HashMap<LocalKey,Inode>, internal_patch_id:&[u8]){
    for change in changes {
        match *change {
            Change::NewNodes { up_context:_, down_context:_,ref line_num,ref flag,ref nodes } => {
                if flag&FOLDER_EDGE != 0 {
                    let mut node=[0;3+KEY_SIZE];
                    unsafe { let _=copy_nonoverlapping(internal_patch_id.as_ptr(), node.as_mut_ptr().offset(3), HASH_SIZE); }
                    let mut l0=*line_num + 1;
                    for i in 0..LINE_SIZE { node[3+HASH_SIZE+i]=(l0&0xff) as u8; l0 = l0>>8 }
                    let mut inode=[0;INODE_SIZE];
                    let inode_l2 = match updates.get(&node[(3+HASH_SIZE)..]) {
                        None => {
                            // This file comes from a remote patch
                            create_new_inode(repo, &mut inode[..]);
                            &inode[..]
                        },
                        Some(ref inode)=>
                            // This file comes from a local patch
                            &inode[..]
                    };

                    unsafe {
                        node[1]=(nodes[0][0] & 0xff) as u8;
                        node[2]=(nodes[0][1] & 0xff) as u8;
                        let mut k = MDB_val { mv_data:inode_l2.as_ptr() as *const c_void, mv_size:INODE_SIZE as size_t };
                        let mut v = MDB_val { mv_data:node.as_ptr() as *const c_void, mv_size:(3+KEY_SIZE) as size_t };
                        mdb_put(repo.mdb_txn,repo.dbi_inodes, &mut k, &mut v, 0);
                        k.mv_data=node.as_ptr().offset(3) as *mut c_void;
                        k.mv_size=KEY_SIZE as size_t;
                        v.mv_data=inode_l2.as_ptr() as *mut c_void;
                        v.mv_size=INODE_SIZE as size_t;
                        mdb_put(repo.mdb_txn,repo.dbi_revinodes, &mut k, &mut v, 0);
                    }
                }
            },
            Change::Edges(_) => {}
        }
    }
}

struct CursIter<'a> {
    cursor:Cursor<'a>,
    initialized:bool,
    key:MDB_val,
    val:MDB_val
}

impl <'a>CursIter<'a> {
    fn new(curs:Cursor<'a>,key:&'a [u8],flag:u8)->CursIter<'a>{
        CursIter { cursor:curs,
                   key:MDB_val{mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t},
                   val:MDB_val{mv_data:[flag].as_ptr() as *const c_void, mv_size:1},
                   initialized:false }
    }
}

impl <'a> Iterator for CursIter<'a> {
    type Item=&'a [u8];
    fn next(&mut self)->Option<&'a[u8]>{
        unsafe {
            let e=mdb_cursor_get(self.cursor.cursor,&mut self.key,&mut self.val,
                                     if self.initialized { MDB_cursor_op::MDB_NEXT_DUP}
                                     else { self.initialized=true;
                                            MDB_cursor_op::MDB_GET_BOTH_RANGE } as c_uint);
            if e==0 { Some(slice::from_raw_parts(self.val.mv_data as *const u8,self.val.mv_size as usize)) } else {None}
        }
    }
}

fn filename_of_inode<'a>(repo:&'a Repository,inode:&[u8],working_copy:&mut PathBuf) {
    let mut v_inode=MDB_val{mv_data:inode.as_ptr() as *const c_void, mv_size:inode.len() as size_t};
    let mut v_next:MDB_val = unsafe {std::mem::zeroed()};
    let mut components=Vec::new();
    loop {
        let e = unsafe {mdb_get(repo.mdb_txn,repo.dbi_revtree,&mut v_inode, &mut v_next)};
        if e==0 {
            components.push(unsafe { slice::from_raw_parts((v_next.mv_data as *const u8).offset(INODE_SIZE as isize),
                                                           (v_next.mv_size as usize-INODE_SIZE)) });
            v_inode.mv_data=v_next.mv_data;
            v_inode.mv_size=v_next.mv_size;
        } else {
            break
        }
    }
    for c in components.iter().rev() {
        working_copy.push(std::str::from_utf8(c).unwrap());
    }
}



pub fn output_repository(repo:&mut Repository, working_copy:&Path) -> Result<(),Error>{
    fn retrieve_paths<'a> (repo:&'a Repository,
                           working_copy:&Path,
                           key:&'a [u8],path:&mut PathBuf,parent_inode:&'a [u8],
                           paths:&mut HashMap<PathBuf,Vec<(&'a[u8],&'a[u8],&'a[u8],PathBuf,usize)>>,
                           cache:&mut HashSet<&'a [u8]>) {
        if !cache.contains(key) {
            cache.insert(key);
            let curs_b=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
            let curs_b_=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
            for b in CursIter::new(curs_b,key,FOLDER_EDGE).chain(CursIter::new(curs_b_,key,FOLDER_EDGE|PSEUDO_EDGE)) {

                let mut bv= unsafe {MDB_val { mv_data:b.as_ptr().offset(1) as *const c_void,
                                              mv_size:KEY_SIZE as size_t }};
                let mut cont_b:MDB_val=unsafe { std::mem::zeroed() };
                let e=unsafe {mdb_get(repo.mdb_txn,repo.dbi_contents,&mut bv,&mut cont_b) };
                if e!=0 || cont_b.mv_size < 2 { panic!("node (b) without a content") } else {
                    let cont_b_data=cont_b.mv_data as *const u8;
                    let filename=unsafe { slice::from_raw_parts(cont_b_data.offset(2),
                                                                (cont_b.mv_size as usize)-2) };
                    let perms= unsafe{((((*cont_b_data) as usize) << 8) | (*(cont_b_data.offset(1)) as usize)) & 0x1ff};
                    unsafe {
                        bv.mv_data=cont_b_data.offset(1) as *const c_void;
                        bv.mv_size=KEY_SIZE as size_t
                    }

                    let curs_c=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                    let curs_c_=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                    for c in CursIter::new(curs_c,key,FOLDER_EDGE).chain(CursIter::new(curs_c_,key,FOLDER_EDGE|PSEUDO_EDGE)) {

                        let mut cv=unsafe {MDB_val { mv_data:(c.as_ptr() as *const u8).offset(1) as *const c_void,
                                                     mv_size:KEY_SIZE as size_t }};
                        let mut cont_c:MDB_val=unsafe { std::mem::zeroed() };
                        let e=unsafe {mdb_get(repo.mdb_txn,repo.dbi_contents,&mut cv,&mut cont_c) };
                        if e!=0 {
                            panic!("c contents not found")
                        } else {
                            let mut inode:MDB_val = unsafe { std::mem::zeroed() };
                            let e=unsafe {mdb_get(repo.mdb_txn,repo.dbi_revinodes,&mut cv,&mut inode) };
                            let inode=
                                if e==0 {
                                    unsafe { slice::from_raw_parts(inode.mv_data as *const u8,
                                                                   inode.mv_size as usize) }
                                } else {
                                    panic!("inodes not synchronized")
                                };
                            {
                                let vec=paths.entry(path.clone()).or_insert(Vec::new());
                                let mut buf=PathBuf::from(working_copy);
                                filename_of_inode(repo,inode,&mut buf);
                                vec.push((c,parent_inode,inode,buf,perms))
                            }
                            if perms & DIRECTORY_FLAG != 0 { // is_directory
                                path.push(std::str::from_utf8(filename).unwrap());
                                retrieve_paths(repo,working_copy,c,path,inode,paths,cache);
                                path.pop();
                            }
                        }
                    }
                }
            }
        }
    }
    let root_key=&ROOT_KEY;
    let root_inode=&ROOT_INODE;
    let mut paths=HashMap::new();
    let mut cache=HashSet::new();
    let mut buf=PathBuf::from(working_copy);
    retrieve_paths(repo,working_copy,root_key,&mut buf,root_inode,&mut paths,&mut cache);
    unsafe {
        mdb_drop(repo.mdb_txn,repo.dbi_tree,0);
        mdb_drop(repo.mdb_txn,repo.dbi_revtree,0);
    };
    for (k,a) in paths {
        let alen=a.len();
        let mut kk=k.clone();
        let mut filename=kk.file_name().unwrap().to_os_string();
        let mut i=0;
        for (node,parent_inode,inode,oldpath,perms) in a {
            if alen>1 { filename.push(format!("~{}",i)) }
            kk.set_file_name(&filename);
            try!(fs::rename(oldpath,&kk));
            unsafe {
                let mut kk=parent_inode.to_vec();
                kk.extend(filename.to_str().unwrap().as_bytes());
                let mut k:MDB_val = MDB_val { mv_data:kk.as_ptr() as *const c_void, mv_size:kk.len() as size_t };
                let mut v:MDB_val = MDB_val { mv_data:inode.as_ptr() as *const c_void, mv_size:inode.len() as size_t };
                mdb_put(repo.mdb_txn,repo.dbi_tree,&mut k,&mut v,0);
                mdb_put(repo.mdb_txn,repo.dbi_revtree,&mut v,&mut k,0);
            }
            // Then (if file) output file
            if perms & DIRECTORY_FLAG == 0 { // this is a real file, not a directory
                let mut l=retrieve(repo,node).unwrap();
                let mut f=std::fs::File::create(&kk).unwrap();
                output_file(repo,&mut f,&mut l);
            }
            //
            i+=1
        }
    }
    Ok(())
}
const DIRECTORY_FLAG:usize = 0x200;

pub fn debug<W>(repo:&mut Repository,w:&mut W) where W:Write {
    let mut styles=Vec::with_capacity(16);
    for i in 0..15 {
        styles.push(("color=").to_string()
                    +["red","blue","green","black"][(i >> 1)&3]
                    +if (i as u8)&DELETED_EDGE!=0 { ", style=dashed"} else {""}
                    +if (i as u8)&PSEUDO_EDGE!=0 { ", style=dotted"} else {""})
    }
    w.write(b"digraph{\n").unwrap();
    let curs=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
    unsafe {
        let mut k:MDB_val=std::mem::zeroed();
        let mut v:MDB_val=std::mem::zeroed();
        let mut e=mdb_cursor_get(curs.cursor,&mut k,&mut v,MDB_cursor_op::MDB_FIRST as c_uint);
        //println!("debug e={}",e);
        let cur=&[][..];
        while e==0 {
            let kk=slice::from_raw_parts(k.mv_data as *const u8,k.mv_size as usize);
            let vv=slice::from_raw_parts(v.mv_data.offset(1) as *const u8,KEY_SIZE as usize);
            if kk!=cur {
                let mut ww:MDB_val=std::mem::zeroed();
                let f=mdb_get(repo.mdb_txn,repo.dbi_contents, &mut k, &mut ww);
                let cont:&[u8]=
                    if f==0 { slice::from_raw_parts(ww.mv_data as *const u8,ww.mv_size as usize) } else { &[][..] };
                write!(w,"n_{}[label=\"{}: {}\"];\n", (&kk).to_hex(), (&kk).to_hex(),
                       match str::from_utf8(&cont) { Ok(x)=>x.to_string(), Err(_)=> (&cont).to_hex() }).unwrap();
            }
            let flag:u8= * (v.mv_data as *const u8);
            write!(w,"n_{}->n_{}[{},label=\"{}\"];\n", (&kk).to_hex(), (&vv).to_hex(), styles[(flag&0xff) as usize], flag).unwrap();
            e=mdb_cursor_get(curs.cursor,&mut k,&mut v,MDB_cursor_op::MDB_NEXT as c_uint);
        }
    }
    w.write(b"}\n").unwrap();
}
