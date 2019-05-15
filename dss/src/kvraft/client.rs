use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::Future;

const TIMEOUT_WAIT_RAFT: u64 = 110;
const TIMEOUT_WAIT_NEXT_LEADER: u64 = 50;

use super::service;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    servers: Vec<service::KvClient>,
    seq: Arc<Mutex<u64>>,
    leader_id: Arc<Mutex<usize>>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<service::KvClient>) -> Clerk {
        Clerk {
            name,
            servers,
            seq: Arc::new(Mutex::new(0)),
            leader_id: Arc::new(Mutex::new(0)),
        }
    }

    pub fn get_seq(&self) -> u64 {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        *seq
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let args = service::GetRequest {
            key: key,
            seq: self.get_seq(), //seq += 1;
            client_name: self.name.clone(),
        };
        let mut leader = *self.leader_id.lock().unwrap();
        loop {
            let ret = self.servers[leader].get(&args.clone()).wait();
            match ret {
                Ok(reply) => {
                    //println!("clerk:{:?}", reply);
                    if reply.err == "OK" {
                        //成功提交，并返回
                        if !reply.wrong_leader {
                            *self.leader_id.lock().unwrap() = leader;
                        }
                        return reply.value;
                    }
                    if !reply.wrong_leader {
                        //正常leader,
                        *self.leader_id.lock().unwrap() = leader;
                        thread::sleep(Duration::from_millis(TIMEOUT_WAIT_RAFT)); //等成功提交
                    } else {
                        //错误leader
                        leader = (leader + 1) % self.servers.len();
                        thread::sleep(Duration::from_millis(TIMEOUT_WAIT_NEXT_LEADER));
                    }
                }
                Err(_) => {
                    leader = (leader + 1) % self.servers.len();
                    thread::sleep(Duration::from_millis(TIMEOUT_WAIT_NEXT_LEADER));
                }
            }
            //leader = (leader + 1) % self.servers.len();
            //thread::sleep(Duration::from_millis(120));
        }

        //unimplemented!()
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let mut key;
        let mut value;
        let for_op;
        let seq = self.get_seq();

        match op {
            Op::Put(k, v) => {
                key = k;
                value = v;
                for_op = service::Op::Put;
            }
            Op::Append(k, v) => {
                key = k;
                value = v;
                for_op = service::Op::Append;
            }
        }

        let args = service::PutAppendRequest {
            key,
            value,
            op: for_op as i32,
            seq,
            client_name: self.name.clone(),
        };
        let mut leader = *self.leader_id.lock().unwrap();
        loop {
            let ret = self.servers[leader]
                .put_append(&args.clone())
                .wait();
            match ret {
                Ok(reply) => {
                    if reply.err == "OK" {
                        //成功提交，并返回
                        if !reply.wrong_leader {
                            *self.leader_id.lock().unwrap() = leader;
                        }
                        return;
                    }
                    if !reply.wrong_leader {
                        *self.leader_id.lock().unwrap() = leader;
                        // wait for raft
                        thread::sleep(Duration::from_millis(TIMEOUT_WAIT_RAFT));
                    } else {
                        leader = (leader + 1) % self.servers.len();
                        // wait for election
                        thread::sleep(Duration::from_millis(TIMEOUT_WAIT_NEXT_LEADER));
                    }
                }
                Err(e) => {
                    leader = (leader + 1) % self.servers.len();
                    thread::sleep(Duration::from_millis(TIMEOUT_WAIT_NEXT_LEADER));
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
