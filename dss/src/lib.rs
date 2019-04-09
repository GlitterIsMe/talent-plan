#![feature(integer_atomics)]
// You need to remove these two allows.
#![allow(dead_code)]
#![allow(unused_variables)]

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate prost_derive;

extern crate rand;

mod kvraft;
mod raft;
