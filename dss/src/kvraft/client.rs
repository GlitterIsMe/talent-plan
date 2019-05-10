use std::fmt;
use std::thread;
use std::sync::mpsc;
use super::service;
use crate::kvraft::service::{GetRequest, PutAppendRequest, GetReply, PutAppendReply};
use labrpc::Error;
use std::time::Duration;

enum Op {
    Put(String, String),
    Append(String, String),
}

enum Response{
    Timeout,
    GetReply(GetReply),
    PutReply(PutAppendReply),
}

static SEQ: u8 = 0;
pub struct Clerk {
    pub name: String,
    servers: Vec<service::KvClient>,

    leader_id: u64,
    id: u64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<service::KvClient>) -> Clerk {
        // You'll have to add code here.
        seq += 1;
        Clerk {
            name,
            servers,
            leader_id: 0,
            id: seq,
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&mut self, key: String) -> String {
        let args = GetRequest{
            key,
            client_id: self.id,
        };
        let mut leader_id = self.leader_id;
        for i in 0..self.servers.len(){
            let (tx, rx) = mpsc::channel();
            let args = args.clone();
            let tx = tx.clone();
            let client = self.servers[leader_id];
            thread::spawn(move ||{
                match client.get(args).wait(){
                    Ok(reply) =>{
                        tx.send(reply);
                    },
                    Err(_) =>{
                        //
                        println!("rpc error");
                    },
                }
            });
            match rx.recv_timeout(Duration::from_millis(200)){
                Ok(res) =>{
                    if reply.err == String::new() && !reply.wrong_leader{
                        self.leader_id = leader_id;
                        return reply.value;
                    }else{
                        leader_id += 1;
                        continue;
                    }
                },

                Err(e) =>{
                    // RPC timeout
                    leader_id += 1;
                    continue;
                }
            }
        }
        "".to_string()
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&mut self, op: Op) {
        let args = match op{
            Op::Put(key, vlaue) =>{
                PutAppendRequest{
                    key,
                    value,
                    op: 0,
                }
            },
            Op::Append(key, value) =>{
                PutAppendRequest{
                    key,
                    value,
                    op: 1,
                }
            }
        };
        let mut leader_id = self.leader_id;
        for i in 0..self.servers.len(){
            let (tx, rx) = mpsc::channel();
            let args = args.clone();
            let tx = tx.clone();
            let client = self.servers[leader_id];
            thread::spawn(move ||{
                match client.put_append(args).wait(){
                    Ok(reply) =>{
                        tx.send(reply);
                    },
                    Err(_) =>{
                        //
                        println!("rpc error");
                    },
                }
            });
            match rx.recv_timeout(Duration::from_millis(200)){
                Ok(res) =>{
                    if reply.err == String::new() && !reply.wrong_leader{
                        self.leader_id = leader_id;
                        break;
                    }else{
                        leader_id += 1;
                        continue;
                    }
                },

                Err(e) =>{
                    // RPC timeout
                    leader_id += 1;
                    continue;
                }
            }
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&mut self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
