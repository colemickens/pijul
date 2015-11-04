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
use self::libc::types::os::arch::posix88::mode_t;
use self::libc::funcs::c95::string::strncpy;
use std::ptr;

use std::slice;
use std::fmt;
use std::str;
use std;
use std::collections::HashMap;
extern crate rand;
use std::path::{PathBuf,Path};
#[allow(missing_copy_implementations)]
pub enum MDB_env {}
pub enum MDB_txn {}
pub enum MDB_cursor {}
use std::io::prelude::*;
use std::io::Error;
use std::marker::PhantomData;

pub mod fs_representation;

type MDB_dbi=c_uint;
#[repr(C)]
pub struct MDB_val {
    pub mv_size:size_t,
    pub mv_data: *const c_void
}
#[repr(C)]
pub enum MDB_cursor_op {
    MDB_FIRST,
    MDB_FIRST_DUP,
    MDB_GET_BOTH,
    MDB_GET_BOTH_RANGE,
    MDB_GET_CURRENT,
    MDB_GET_MULTIPLE,
    MDB_LAST,
    MDB_LAST_DUP,
    MDB_NEXT,
    MDB_NEXT_DUP,
    MDB_NEXT_MULTIPLE,
    MDB_NEXT_NODUP,
    MDB_PREV,
    MDB_PREV_DUP,
    MDB_PREV_NODUP,
    MDB_SET,
    MDB_SET_KEY,
    MDB_SET_RANGE
}

extern "C" {
    fn mdb_env_create(env: *mut *mut MDB_env) -> c_int;
    fn mdb_env_open(env: *mut MDB_env, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    fn mdb_env_close(env: *mut MDB_env);
    fn mdb_env_set_maxdbs(env: *mut MDB_env,maxdbs:c_uint)->c_int;
    fn mdb_env_set_mapsize(env: *mut MDB_env,mapsize:size_t)->c_int;
    fn mdb_reader_check(env:*mut MDB_env,dead:*mut c_int)->c_int;
    fn mdb_txn_begin(env: *mut MDB_env,parent: *mut MDB_txn, flags:c_uint, txn: *mut *mut MDB_txn)->c_int;
    fn mdb_txn_commit(txn: *mut MDB_txn)->c_int;
    fn mdb_txn_abort(txn: *mut MDB_txn);
    fn mdb_dbi_open(txn: *mut MDB_txn, name: *const c_char, flags:c_uint, dbi:*mut MDB_dbi)->c_int;
    fn mdb_get(txn: *mut MDB_txn, dbi:MDB_dbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    fn mdb_put(txn: *mut MDB_txn, dbi:MDB_dbi, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_get(cursor: *mut MDB_cursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_put(cursor: *mut MDB_cursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_del(cursor: *mut MDB_cursor, flags:c_uint)->c_int;
    fn mdb_cursor_open(txn: *mut MDB_txn, dbi:MDB_dbi, cursor:*mut *mut MDB_cursor)->c_int;
    fn mdb_cursor_close(cursor: *mut MDB_cursor);
}


pub const MDB_REVERSEKEY: c_uint = 0x02;
pub const MDB_DUPSORT: c_uint = 0x04;
pub const MDB_INTEGERKEY: c_uint = 0x08;
pub const MDB_DUPFIXED: c_uint = 0x10;
pub const MDB_INTEGERDUP: c_uint = 0x20;
pub const MDB_REVERSEDUP: c_uint =  0x40;
pub const MDB_CREATE: c_uint = 0x40000;
pub const MDB_NOTFOUND: c_int = -30798;


const MAX_DBS:usize=10;
pub enum DBI {
    NODES,
    CONTENTS,
    REVDEP,
    INTERNAL_HASHES,
    EXTERNAL_HASHES,
    BRANCHES,
    TREE,
    REVTREE,
    INODES,
    REVINODES
}

const dbis:[(&'static str,c_uint);MAX_DBS]=[("nodes\0",MDB_CREATE|MDB_DUPSORT),
                                            ("contents\0",MDB_CREATE),
                                            ("revdep\0",MDB_CREATE|MDB_DUPSORT),
                                            ("internal\0",MDB_CREATE),
                                            ("external\0",MDB_CREATE),
                                            ("branches\0",MDB_CREATE|MDB_DUPSORT),
                                            ("tree\0",MDB_CREATE|MDB_DUPSORT),
                                            ("revtree\0",MDB_CREATE),
                                            ("inodes\0",MDB_CREATE),
                                            ("revinodes\0",MDB_CREATE)
                                            ];

pub struct Repository{
    mdb_env:*mut MDB_env,
    mdb_txn:*mut MDB_txn,
    dbi_:[Option<MDB_dbi>;MAX_DBS]
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
            let e=mdb_env_set_maxdbs(env,MAX_DBS as c_uint);
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

                    let repo=Repository{
                        mdb_env:env,
                        mdb_txn:txn,
                        dbi_:[None;MAX_DBS]
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
    fn dbi(&mut self,num:DBI)->MDB_dbi{
        let n=num as usize;
        match self.dbi_[n] {
            Some(dbi) => {dbi},
            None => {
                unsafe {
                    let d=0;
                    let (name,flag)=dbis[n];
                    unsafe {mdb_dbi_open(self.mdb_txn,name.as_ptr() as *const c_char,flag,std::mem::transmute(&d))};
                    self.dbi_[n]=Some(d);
                    d
                }
            }
        }
    }
}

impl Drop for Repository {
    fn drop(&mut self){
        unsafe {
            mdb_txn_abort(self.mdb_txn);
            mdb_env_close(self.mdb_env)
        }
    }
}

const INODE_SIZE:usize=16;
const HASH_SIZE:usize=20;
const LINE_SIZE:usize=4;
const KEY_SIZE:usize=HASH_SIZE+LINE_SIZE;
const ROOT_INODE:[u8;INODE_SIZE]=[0;INODE_SIZE];
const ROOT_KEY:[u8;KEY_SIZE]=[0;KEY_SIZE];


fn add_inode(repo:&mut Repository, inode:&Option<[c_char;INODE_SIZE]>, path:&std::path::Path)->Result<(),()>{
    let mut buf:Vec<c_char>=Vec::with_capacity(INODE_SIZE);
    // Init to 0
    for i in 0..INODE_SIZE {
        buf.push(0)
    }
    let mut components=path.components();
    let mut cs=components.next();
    while let Some(s)=cs { // need to peek at the next element, so no for.
        cs=components.next();
        match s.as_os_str().to_str(){
            Some(ss) => {
                buf.truncate(INODE_SIZE);
                for c in ss.as_bytes() { buf.push(*c as c_char) }
                let mut k=MDB_val { mv_data:buf.as_ptr() as *mut c_void, mv_size:buf.len()as size_t };
                let mut v=MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
                let ret= unsafe { mdb_get(repo.mdb_txn,repo.dbi(DBI::TREE),&mut k,&mut v) };
                if ret==0 {
                    // replace buf with existing inode
                    buf.clear();
                    let pv:*const c_char=v.mv_data as *const c_char;
                    unsafe { for c in 0..v.mv_size { buf.push(*pv.offset(c as isize)) } }
                } else {
                    let inode = if cs.is_none() && inode.is_some() {
                        inode.unwrap()
                    } else {
                        let mut inode:[c_char;INODE_SIZE]=[0;INODE_SIZE];
                        for i in 0..INODE_SIZE { inode[i]=rand::random() }
                        inode
                    };
                    v.mv_data=inode.as_ptr() as *const c_void;
                    v.mv_size=INODE_SIZE as size_t;
                    unsafe { mdb_put(repo.mdb_txn,repo.dbi(DBI::TREE),&mut k,&mut v,0) };
                    unsafe { mdb_put(repo.mdb_txn,repo.dbi(DBI::REVTREE),&mut v,&mut k,0) };
                    if cs.is_some() {
                        k.mv_data="".as_ptr() as *const c_void;
                        k.mv_size=0;
                        unsafe { mdb_put(repo.mdb_txn,repo.dbi(DBI::TREE),&mut v,&mut k,0) };
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

pub fn add_file(repo:&mut Repository, path:&std::path::Path)->Result<(),()>{
    add_inode(repo,&None,path)
}


pub enum Change {
    NewNodes{
        up_context:Vec<Vec<u8>>,
        down_context:Vec<Vec<u8>>,
        flag:u8,
        line_num:usize,
        nodes:Vec<Vec<u8>>
    },
    Edges(Vec<(Vec<u8>, Vec<u8>, u8, Vec<u8>)>)
}

struct Cursor {
    cursor:*mut MDB_cursor,
}

impl Cursor {
    fn new(txn:*mut MDB_txn,dbi:MDB_dbi)->Result<Cursor,Error>{
        unsafe {
            let curs=ptr::null_mut();
            let e=mdb_cursor_open(txn,dbi,std::mem::transmute(&curs));
            if e!=0 { Err(Error::from_raw_os_error(e)) } else { Ok(Cursor { cursor:curs }) }
        }
    }
}
impl Drop for Cursor {
    fn drop(&mut self){
        unsafe {mdb_cursor_close(self.cursor);}
    }
}

const LINE_VISITED:c_uchar=8;
const LINE_ONSTACK:c_uchar=4;
const LINE_SPIT:c_uchar=2;

#[repr(C)]
struct c_line {
    key:*const char,
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
    // anything).
    fn c_retrieve(txn:*mut MDB_txn,dbi:MDB_dbi,key:*const c_char) -> *mut c_line;
    fn c_free_line(c_line:*mut c_line);
}

struct Line { c_line:*mut c_line }
impl Drop for Line {
    fn drop(&mut self){
        unsafe {c_free_line(self.c_line)}
    }
}
fn retrieve(repo:&mut Repository,key:&[u8])->Result<Line,()>{
    unsafe {
        let c_line=c_retrieve(repo.mdb_txn,repo.dbi(DBI::NODES),key.as_ptr() as *const c_char);
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
            let children:&[*mut c_line]=slice::from_raw_parts((*l).children, (*l).children_off as usize);
            for child in children {
                if (**child).flags & LINE_VISITED == 0 {
                    dfs(stack,index,*child);
                    (*l).lowlink=std::cmp::min((*l).lowlink, (**child).lowlink);
                } else {
                    if (**child).flags & LINE_ONSTACK != 0 {
                        (*l).lowlink=std::cmp::min((*l).lowlink, (**child).index)
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

struct File<'a> {
    counts:Vec<usize>,
    lines:Vec<Vec<*mut c_line>>,
    i:usize,
    phantom: PhantomData<&'a()>
}

impl<'a> File<'a> {
    fn new(file:&'a mut Line)->File<'a> {
        let max_level=tarjan(file);
        let mut counts=vec![0;max_level+1];
        let mut lines:Vec<Vec<*mut c_line>>=vec![vec!();max_level+1];
        for i in 0..counts.len() { lines[i]=Vec::new() }

        // First task: add number of lines and list of lines for each level.
        fn fill_lines(counts:&mut Vec<usize>,
                      lines:&mut Vec<Vec<*mut c_line>>,
                      cl:*mut c_line){
            unsafe {
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

        // Then add "undetected conflicts"
        unsafe  {
            for i in 0..counts.len() {
                if *counts.get_unchecked(i) > 1 {
                    for line in lines.get_unchecked(i) {
                        let children:&[*mut c_line]=slice::from_raw_parts((**line).children, (**line).children_off as usize);
                        for child in children {
                            for j in (**line).lowlink+1 .. (**child).lowlink-1 {
                                (*counts.get_unchecked_mut(i)) += 1
                            }}}}}
        }
        File {
            counts:counts,
            lines:lines,
            i:0,
            phantom:PhantomData
        }
    }
}


impl <'a> Iterator for File<'a> {
    type Item = Vec<&'a[u8]>;
    fn next(&mut self)->Option<Vec<&'a[u8]>>{
        if self.i >= self.counts.len() { None } else {
            unsafe {
                if *self.counts.get_unchecked(self.i) == 1 {
                    let l = self.lines.get_unchecked(self.i) [0];
                    Some(vec!(slice::from_raw_parts((*l).key as *const u8, KEY_SIZE)))
                } else {
                    // conflit
                    unimplemented!()
                }
            }
        }
    }
}


fn contents<'a>(repo:&mut Repository, key:&'a[u8])->&'a[u8] {
    unsafe {
        let mdb_txn=repo.mdb_txn;
        let mdb_dbi=repo.dbi(DBI::CONTENTS);
        let mut k = MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
        let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let e=mdb_get(mdb_txn, mdb_dbi, &mut k, &mut v);
        if e==0 {
            slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)
        } else {
            &[][..]
        }
    }
}

fn push_conflict<'a>(repo:&mut Repository,lines_a:&mut Vec<&'a[u8]>, l:Vec<&'a[u8]>) {
    if l.len()==1 {
        lines_a.push(contents(repo,&l[0]))
    } else {
        unimplemented!()
    }
}

fn external_key(repo:&mut Repository,key:&[u8])->Vec<u8> {
    unsafe {
        let mut k = MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:HASH_SIZE as size_t };
        let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let e = unsafe { mdb_get(repo.mdb_txn,repo.dbi(DBI::INODES),&mut k, &mut v) };
        if e==0 {
            let mut result:Vec<u8>=Vec::with_capacity(v.mv_size as usize+LINE_SIZE);
            let pv=slice::from_raw_parts(v.mv_data as *const c_uchar, v.mv_size as usize);
            for c in pv { result.push(*c) }
            for c in &key[HASH_SIZE..KEY_SIZE] { result.push(*c) }
            result
        } else {
            panic!("external key not found !")
        }
    }
}

fn delete_edges<'a>(repo:&mut Repository, edges:&mut Vec<(Vec<u8>,Vec<u8>,u8,Vec<u8>)>, key:&'a[u8]){
    // Get external key for "key"
    let ext_key=external_key(repo,key);

    // Then collect edges to delete
    let curs_tree=Cursor::new(repo.mdb_txn,repo.dbi(DBI::TREE)).unwrap();
    for c in [PARENT_EDGE, PARENT_EDGE|FOLDER_EDGE].iter() {
        unsafe {
            let mut k = MDB_val{ mv_data:key.as_ptr() as *const c_void, mv_size:key.len()as size_t };
            let mut v = MDB_val{ mv_data:(c as *const c_uchar) as *const c_void, mv_size:1 };
            let mut e= mdb_cursor_get(curs_tree.cursor, &mut k,&mut v,MDB_cursor_op::MDB_GET_BOTH_RANGE as c_uint);
            // take all parent or folder-parent edges:
            while e==0 && v.mv_size>0 && *(v.mv_data as (*mut c_uchar)) == *c {
                if (v.mv_size as usize) < 1+HASH_SIZE+KEY_SIZE {
                    panic!("Wrong encoding in delete_edges")
                }
                // look up the external hash up.
                let pv=slice::from_raw_parts((v.mv_data as *const c_uchar).offset(1), KEY_SIZE as usize);
                let pp=slice::from_raw_parts((v.mv_data as *const c_uchar).offset(1+KEY_SIZE as isize), HASH_SIZE as usize);
                edges.push((ext_key.clone(), external_key(repo,pv), *c, external_key(repo,pp)));
                e= mdb_cursor_get(curs_tree.cursor, &mut k,&mut v,MDB_cursor_op::MDB_NEXT_DUP as c_uint);
            }
        }
    }
}

fn diff(repo:&mut Repository,line_num:&mut usize, actions:&mut Vec<Change>, a:&mut Line, b:&Path) {
    let mut lines_a=Vec::new();
    let it=File::new(a);
    for l in it {
        push_conflict(repo, &mut lines_a, l)
    }

    let mut buf_b=Vec::new();
    let mut lines_b=Vec::new();
    {
        let f = std::fs::File::open(b);
        let mut f = std::io::BufReader::new(f.unwrap());
        f.read_to_end(&mut buf_b);
        let mut i=0;
        let mut j=0;
        while j<buf_b.len() {
            if buf_b[j]==0xa {
                lines_b.push(&buf_b[i..j+1]);
                i=j+1
            }
            j+=1;
        }
    };
    fn local_diff(repo:&mut Repository,actions:&mut Vec<Change>,line_num:&mut usize, a:&[&[u8]], b:&[&[u8]]) {
        let mut opt:Vec<Vec<usize>>=Vec::with_capacity(a.len()+1);
        for i in 0..opt.len() { opt.push (vec![0;b.len()+1]) }
        // opt
        for i in (0..a.len()).rev() {
            for j in (0..b.len()).rev() {
                opt[i][j]=
                    if a[i]==b[i] { opt[i+1][j+1]+1 } else { std::cmp::max(opt[i+1][j], opt[i][j+1]) }
            }
        }
        let mut i=1;
        let mut j=0;
        fn add_lines(actions:&mut Vec<Change>, line_num:&mut usize,
                     up_context:&[u8],down_context:&[&[u8]],lines:&[&[u8]]){
            actions.push(
                Change::NewNodes {
                    up_context:vec!(up_context.to_vec()),
                    down_context:{ let mut d=Vec::with_capacity(down_context.len());
                                   for c in down_context { d.push(c.to_vec()) };
                                   d },
                    line_num: *line_num,
                    flag:0,
                    nodes:{
                        let mut nodes=Vec::with_capacity(lines.len());
                        for l in lines { nodes.push(l.to_vec()) }
                        nodes
                    }
                });
            *line_num += lines.len()
        }
        fn delete_lines(repo:&mut Repository,actions:&mut Vec<Change>, lines:&[&[u8]]){
            let mut edges=Vec::new();
            for l in lines {
                delete_edges(repo,&mut edges,l)
            }
            actions.push(Change::Edges(edges))
        }
        while(i<a.len() && j<b.len()) {
            if a[i]==b[i] { i+=1; j+=1 }
            else {
                let i0=i;
                while(i<a.len() && opt[i+1][j]>=opt[i][j+1]) { i+=1 };
                if i>i0 { delete_lines(repo,actions, &a[i0..i]) }
                if i<a.len() {
                    let j0=j;
                    while(j<b.len() && opt[i+1][j] < opt[i][j+1]) { j+=1 };
                    if j>j0 { add_lines(actions, line_num, a[i], if i<a.len() {&a[i..i+1]} else { &[][..] }, &b[j0..j]) }
                }
            }
        }
        if i < a.len() { delete_lines(repo,actions, &a[i..a.len()]) }
        else if j < b.len() { add_lines(actions, line_num, a[i-1], &[][..], &b[j..b.len()]) }
    }
    local_diff(repo,actions, line_num, &lines_a[..],&lines_b[..])
}




const PSEUDO_EDGE:u8=1;
const FOLDER_EDGE:u8=2;
const PARENT_EDGE:u8=4;
const DELETED_EDGE:u8=8;

pub fn record(repo:&mut Repository,working_copy:&std::path::Path)->Result<Vec<Change>,Error>{
    // no recursive closures, but I understand why (ownership would be tricky).
    fn dfs(repo:&mut Repository, actions:&mut Vec<Change>,
           line_num:&mut usize,updatables:&HashMap<&[u8],&[u8]>,
           parent_inode:Option<&[u8]>,
           parent_node:Option<&[u8]>,
           current_inode:&[u8],
           realpath:&mut std::path::PathBuf, basename:&[u8]) -> Result<(),Error> {

        realpath.push(str::from_utf8(&basename).unwrap());

        let mut k = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let root_key=&ROOT_KEY[..];
        let mut l2=[0;LINE_SIZE];
        let current_node=
            match parent_inode {
                Some(parent_inode) => {
                    k.mv_data=current_inode.as_ptr() as *const c_void;
                    k.mv_size=INODE_SIZE as size_t;
                    let e = unsafe { mdb_get(repo.mdb_txn,repo.dbi(DBI::INODES),&mut k, &mut v) };
                    if e==0 { // This inode already has a corresponding node
                        let current_node=unsafe { slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize) };
                        if current_node[0]==1 {
                            // file moved
                        } else if current_node[0]==2 {
                            // file deleted. delete recursively
                        } else if current_node[0]==0 {
                            // file not moved, we need to diff
                            let ret=retrieve(repo,&current_node);
                            diff(repo,line_num,actions, &mut ret.unwrap(), realpath.as_path())
                        } else {
                            panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                        };
                        current_node
                    } else {
                        // File addition, create appropriate Newnodes.
                        let mut nodes=Vec::new();
                        let mut lnum= *line_num;
                        for i in 0..(LINE_SIZE-1) { l2[i]=(lnum & 0xff) as u8; lnum=lnum>>8 }
                        actions.push(
                            Change::NewNodes { up_context: vec!(parent_node.unwrap().to_vec()),
                                               line_num: *line_num,
                                               down_context: vec!(),
                                               nodes: vec!(basename.to_vec(),vec!()),
                                               flag:FOLDER_EDGE }
                            );
                        *line_num += 2;

                        // Reading the file
                        nodes.clear();
                        let mut line=Vec::new();
                        let f = std::fs::File::open(realpath.as_path());
                        let mut f = std::io::BufReader::new(f.unwrap());
                        loop {
                            match f.read_until('\n' as u8,&mut line) {
                                Ok(l) => nodes.push(line.clone()),
                                Err(_) => break
                            }
                        }
                        let len=nodes.len();
                        actions.push(
                            Change::NewNodes { up_context:vec!(l2.to_vec()),
                                               line_num: *line_num,
                                               down_context: vec!(),
                                               nodes: nodes,
                                               flag:0 }
                            );
                        *line_num+=len;
                        &l2[..]
                    }
                },
                None => { root_key }
            };
        let curs_tree=try!(Cursor::new(repo.mdb_txn,repo.dbi(DBI::TREE)));
        let mut e= unsafe { mdb_cursor_get(curs_tree.cursor, &mut k,&mut v,MDB_cursor_op::MDB_SET_RANGE as c_uint) };
        while e==0 && unsafe { libc::strncmp(v.mv_data as *const c_char, current_inode.as_ptr() as *const c_char, INODE_SIZE as size_t) } == 0 {

            assert!(v.mv_size as usize==INODE_SIZE);

            if k.mv_size>INODE_SIZE as size_t {
                let (_,next_basename)=(unsafe {slice::from_raw_parts(k.mv_data as *const u8,k.mv_size as usize)}).split_at(INODE_SIZE);
                let _=
                    dfs(repo, actions,line_num,&updatables,
                        Some(current_inode), // parent_inode
                        Some(current_node), // parent_node
                        unsafe {slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)},// current_inode
                        realpath,
                        next_basename);
            }

            e=unsafe { mdb_cursor_get(curs_tree.cursor,&mut k,&mut v,MDB_cursor_op::MDB_NEXT as c_uint) };
        }
        let _=realpath.pop();
        Ok(())
    };
    let mut actions:Vec<Change>=Vec::new();
    let mut line_num=1;
    let updatables:HashMap<&[u8],&[u8]>=HashMap::new();
    let mut realpath=PathBuf::from("/tmp/test");
    dfs(repo,&mut actions,&mut line_num,&updatables,
        None,None,&ROOT_INODE[..],&mut realpath, "test".as_bytes());
    Ok(actions)
}







// Missing: apply, output_repository
