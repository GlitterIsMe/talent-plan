use super::service::*;
use crate::raft;
use futures::stream::Stream;
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use futures::Async;
use labrpc::RpcFuture;
use std::thread;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[macro_export]
macro_rules! myprintln {
    ($($arg: tt)*) => (
        //println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

const CHECK_TIMEOUT: u64 = 100; //for seconds

#[derive(Clone, PartialEq, Message)]
pub struct LatestReply {
    #[prost(uint64, tag = "1")]
    pub seq: u64,

    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Operation {

    #[prost(uint64, tag = "1")]
    pub seq: u64,

    #[prost(string, tag = "2")]
    pub client_name: String,
    //
    #[prost(uint64, tag = "3")]
    pub op: u64,

    #[prost(string, tag = "4")]
    pub key: String,
    //
    #[prost(string, tag = "5")]
    pub value: String, //
}

#[derive(Clone, PartialEq, Message)]
pub struct Snapshot {
    #[prost(uint64, tag = "1")]
    pub snapshot_index: u64,

    #[prost(bytes, repeated, tag = "2")]
    pub db_key: Vec<Vec<u8>>,

    #[prost(bytes, repeated, tag = "3")]
    pub db_value: Vec<Vec<u8>>,

    #[prost(bytes, repeated, tag = "4")]
    pub latest_requests_key: Vec<Vec<u8>>,

    #[prost(bytes, repeated, tag = "5")]
    pub latest_requests_value: Vec<Vec<u8>>,

}

pub struct KvServer {
    pub rf: raft::Node,
    pub me: usize,

    pub max_raft_state: Option<usize>,

    pub apply_ch: UnboundedReceiver<raft::ApplyMsg>,

    pub db: HashMap<String, String>,

    pub latest_requests: HashMap<String, LatestReply>,

    pub snapshot_index: u64,
}

impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        max_raft_state: Option<usize>,
    ) -> KvServer {
        let snapshot = persister.snapshot();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let mut kvserver = KvServer {
            me,
            max_raft_state,
            rf: raft::Node::new(rf),
            apply_ch,
            db: HashMap::new(),
            //ack: HashMap::new(),
            latest_requests: HashMap::new(),
            snapshot_index: 0,
        };
        kvserver.read_snapshot(snapshot);
        kvserver
    }
    pub fn get_state(&self) -> raft::State {
        self.rf.get_state()
    }
    pub fn creat_snapshot(&self) -> Vec<u8> {
        let mut data = vec![];
        let mut snapshot = Snapshot {
            snapshot_index: self.snapshot_index,
            db_key: vec![],
            db_value: vec![],
            latest_requests_key: vec![],
            latest_requests_value: vec![],
        };
        for (key, value) in &self.db {
            let mut db_key = vec![];
            let mut db_value = vec![];
            let _ret = labcodec::encode(&key.clone(), &mut db_key);
            let _ret2 = labcodec::encode(&value.clone(), &mut db_value);
            snapshot.db_key.push(db_key);
            snapshot.db_value.push(db_value);
        }
        for (key, value) in &self.latest_requests {
            let mut latest_requests_key = vec![];
            let mut latest_requests_value = vec![];
            let _ret = labcodec::encode(&key.clone(), &mut latest_requests_key);
            let _ret2 = labcodec::encode(&value.clone(), &mut latest_requests_value);
            snapshot.latest_requests_key.push(latest_requests_key);
            snapshot.latest_requests_value.push(latest_requests_value);
        }
        let _ret = labcodec::encode(&snapshot, &mut data);
        data
    }
    pub fn read_snapshot(&mut self, data: Vec<u8>) {
        if data.is_empty() {
            return;
        }
        match labcodec::decode(&data) {
            Ok(snapshot) => {
                let snapshot: Snapshot = snapshot;
                self.snapshot_index = snapshot.snapshot_index;
                myprintln!("server:{} read snapshot snapshot_index:{}", self.me, self.snapshot_index);
                self.db.clear();
                self.latest_requests.clear();
                for i in 0..snapshot.db_key.len() {
                    let mut db_key: String;
                    let mut db_value: String;
                    match labcodec::decode(&snapshot.db_key[i].clone()) {
                        Ok(key) => {
                            let key: String = key;
                            db_key = key.clone();
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        }
                    }
                    match labcodec::decode(&snapshot.db_value[i].clone()) {
                        Ok(value) => {
                            let value: String = value;
                            db_value = value.clone();
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        }
                    }
                    self.db.insert(db_key.clone(), db_value.clone());
                    myprintln!("server:{} read snapshot db:{}:{}", self.me, db_key, db_value);
                }
                for i in 0..snapshot.latest_requests_key.len() {
                    let mut latest_requests_key: String;
                    let mut latest_requests_value: LatestReply;
                    match labcodec::decode(&snapshot.latest_requests_key[i].clone()) {
                        Ok(key) => {
                            let key: String = key;
                            latest_requests_key = key.clone();
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        }
                    }
                    match labcodec::decode(&snapshot.latest_requests_value[i].clone()) {
                        Ok(value) => {
                            let value: LatestReply = value;
                            latest_requests_value = value.clone();
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        }
                    }
                    self.latest_requests.insert(latest_requests_key.clone(), latest_requests_value.clone());
                    myprintln!("server:{} read snapshot requests:{}:{:?}", self.me, latest_requests_key, latest_requests_value);
                }
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }
    pub fn save_snapshot(&self) {  //kvserver保存snapshot
        let data = self.creat_snapshot();
        self.rf.save_state_and_snapshot(data);
    }
    pub fn if_need_let_raft_compress_log(&self) -> bool {  //是否要让raft检测需要压缩日志
        if self.max_raft_state.is_some() {
            if self.max_raft_state.unwrap() > 0 {
                return true;
            }
        }
        return false;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    pub me: usize,
    pub server: Arc<Mutex<KvServer>>,
    shutdown: Arc<Mutex<bool>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let node = Node {
            me: kv.me,
            server: Arc::new(Mutex::new(kv)),
            shutdown: Arc::new(Mutex::new(false)),
        };
        let node_clone = node.clone();
        thread::spawn(move || {
            //let now = Instant::now();
            loop {
                if *node_clone.shutdown.lock().unwrap() == true {
                    myprintln!("server:{} shutdown apply",node_clone.get_id());
                    break;
                }
                if let Ok(Async::Ready(Some(apply_msg))) =
                futures::executor::spawn(futures::lazy(|| {
                    node_clone.server.lock().unwrap().apply_ch.poll()
                }))
                    .wait_future()
                {
                    if !apply_msg.command_valid {
                        continue;
                    }
                    let mut server = node_clone.server.lock().unwrap();
                    if apply_msg.is_snapshot {  //说明是snapshot应用
                        if server.snapshot_index < apply_msg.command_index {  //接收到更新的snapshot_index
                            server.read_snapshot(apply_msg.snapshot.clone());
                        }
                        continue;
                    }
                    if apply_msg.command.is_empty() || apply_msg.command_index <= server.snapshot_index {
                        continue;   //如果index比snapshot_index更小，不允许重复apply
                    }
                    let command_index = apply_msg.command_index;
                    let mut entry: Operation;
                    match labcodec::decode(&apply_msg.command) {
                        Ok(en) => {
                            entry = en;
                        }
                        Err(e) => {
                            myprintln!("error:decode error");
                            continue;
                        }
                    }
                    myprintln!(
                        "server:{} command_index:{} {:?}",
                        node_clone.get_id(),
                        command_index,
                        entry
                    );
                    if server.latest_requests.get(&entry.client_name).is_none()
                        || server.latest_requests.get(&entry.client_name).unwrap().seq < entry.seq
                    {
                        //可更新
                        let mut reply = LatestReply {
                            seq: entry.seq,
                            value: String::new(),
                        };
                        match entry.op {
                            1 => {
                                //get
                                if server.db.get(&entry.key).is_some() {
                                    reply.value = server.db.get(&entry.key).unwrap().clone();
                                }
                                server.latest_requests.insert(entry.client_name.clone(), reply.clone());
                                myprintln!("server:{} client:{} apply:{:?}", server.me, entry.client_name, reply);
                            }
                            2 => {
                                //put
                                server.db.insert(entry.key.clone(), entry.value.clone());
                                server.latest_requests.insert(entry.client_name.clone(), reply.clone());
                                myprintln!("server:{} client:{} apply:{:?}", server.me, entry.client_name, reply);
                            }
                            3 => {
                                //append
                                if server.db.get(&entry.key).is_some() {
                                    server.db.get_mut(&entry.key).unwrap().push_str(&entry.value.clone());
                                } else {
                                    server.db.insert(entry.key.clone(), entry.value.clone());
                                }
                                server.latest_requests.insert(entry.client_name.clone(), reply.clone());
                                myprintln!("server:{} client:{} apply:{:?}", server.me, entry.client_name, reply);
                            }
                            _ => {
                                //error
                                myprintln!("error:apply op error");
                                continue;
                            }
                        }
                    }
                    server.snapshot_index = command_index;
                    server.save_snapshot();   //kv_server借助raft存储snapshot
                    if server.if_need_let_raft_compress_log() {
                        let max_raft_state: usize = server.max_raft_state.unwrap();
                        server.rf.check_and_do_compress_log(max_raft_state, command_index);
                    }
                }
            }

        });
        node
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.server.lock().unwrap().rf.kill();
        *self.shutdown.lock().unwrap() = true;
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.lock().unwrap().get_state()
    }
    pub fn get_id(&self) -> usize {
        self.me
    }
}

impl KvService for Node {
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        myprintln!("server:{} get:{:?}", self.get_id(), arg);
        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }

        {
            let server = self.server.lock().unwrap();
            if let Some(re) = server.latest_requests.get(&arg.client_name) {
                if arg.seq < re.seq {
                    reply.wrong_leader = true;
                    reply.err = String::from("old seq");
                    myprintln!("kverror:server:{}:{} client:{}:{} get reply:{:?}", self.get_id(), re.seq, arg.client_name, arg.seq, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                } else if arg.seq == re.seq {  //可直接返回结果
                    reply.wrong_leader = false;
                    reply.err = String::from("OK");
                    reply.value = re.value.clone();
                    myprintln!("server:{} client:{} get reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            let cmd = Operation {
                seq: arg.seq,
                client_name: arg.client_name.clone(),
                op: 1,
                key: arg.key.clone(),
                value: String::new(),
            };
            match server.rf.start(&cmd) {
                Ok((index1, term1)) => {  //发送成功
                    reply.wrong_leader = false;
                    myprintln!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
                    //myprintln!("server:{} client:{} get start reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }
                Err(_) => {
                    reply.wrong_leader = true;
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
        }
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        myprintln!("server:{} putappend:{:?}", self.get_id(), arg);
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        {

            let server = self.server.lock().unwrap();
            if let Some(re) = server.latest_requests.get(&arg.client_name) {
                if arg.seq < re.seq {
                    reply.wrong_leader = true;
                    reply.err = String::from("OK");
                    myprintln!("kverror:server:{}:{} client:{}:{} putappend reply:{:?}", self.get_id(), re.seq, arg.client_name, arg.seq, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                } else if arg.seq == re.seq {
                    reply.wrong_leader = false;
                    reply.err = String::from("OK");
                    myprintln!("server:{} client:{} putappend reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            let cmd = Operation {
                seq: arg.seq,
                client_name: arg.client_name.clone(),
                op: (arg.op + 1) as u64,
                key: arg.key.clone(),
                value: arg.value.clone(),
            };
            match server.rf.start(&cmd) {
                Ok((index1, term1)) => {
                    reply.wrong_leader = false;
                    myprintln!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
                    //myprintln!("server:{} client:{} get start reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }
                Err(_) => {
                    reply.wrong_leader = true;
                    reply.err = String::from("");
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
        }
    }
}