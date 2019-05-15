use super::service::*;
use crate::raft;
use futures::stream::Stream;
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use futures::Async;
use labrpc::RpcFuture;
use std::thread;

use std::collections::HashMap;
//use std::sync::mpsc;
//se std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
//use std::time::Instant;
#[macro_export]
macro_rules! kv_debug {
    ($($arg: tt)*) => (
        //println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}

const WAIT_CHECK_TIME: u64 = 100; //单位秒

#[derive(Clone, PartialEq, Message)]
pub struct LatestReply {
    #[prost(uint64, tag = "1")]
    pub seq: u64,      //请求seq

    #[prost(string, tag = "2")]
    pub value: String, //get操作时的结果
}

#[derive(Clone, PartialEq, Message)]
pub struct OpEntry { //日志结构
#[prost(uint64, tag = "1")]
pub seq: u64, //操作的id
#[prost(string, tag = "2")]
pub client_name: String, //
#[prost(uint64, tag = "3")]
pub op: u64, //1代表get，2代表put，3代表append
#[prost(string, tag = "4")]
pub key: String, //
#[prost(string, tag = "5")]
pub value: String, //
}

#[derive(Clone, PartialEq, Message)]
pub struct Snapshot {  //保存快照结构
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
    // snapshot if log grows this big
    pub maxraftstate: Option<usize>,
    // Your definitions here.
    pub apply_ch: UnboundedReceiver<raft::ApplyMsg>,

    pub db: HashMap<String, String>,                   //数据库存储
    //pub ack: HashMap<u64, (String, u64)>,                // 日志index <-> (client_name,seq)
    pub latest_requests: HashMap<String, LatestReply>, //client_name <-> ( 回复;对应客户端的最新回复)

    pub snapshot_index: u64,
}

impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.
        let snapshot = persister.snapshot();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let mut kvserver = KvServer {
            me,
            maxraftstate,
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
        for (key,value) in &self.db {
            let mut db_key = vec![];
            let mut db_value = vec![];
            let _ret = labcodec::encode(&key.clone(), &mut db_key);
            let _ret2 = labcodec::encode(&value.clone(), &mut db_value);
            snapshot.db_key.push(db_key);
            snapshot.db_value.push(db_value);
        }
        for (key,value) in &self.latest_requests {
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
                kv_debug!("server:{} read snapshot snapshot_index:{}", self.me, self.snapshot_index);
                self.db.clear();
                self.latest_requests.clear();
                for i in 0..snapshot.db_key.len() {
                    let mut db_key: String;
                    let mut db_value: String;
                    match labcodec::decode(&snapshot.db_key[i].clone()) {
                        Ok(key) => {
                            let key: String = key;
                            db_key = key.clone();
                        },
                        Err(e) => {
                            panic!("{:?}", e);
                        },
                    }
                    match labcodec::decode(&snapshot.db_value[i].clone()) {
                        Ok(value) => {
                            let value: String = value;
                            db_value = value.clone();
                        },
                        Err(e) => {
                            panic!("{:?}", e);
                        },
                    }
                    self.db.insert(db_key.clone(), db_value.clone());
                    kv_debug!("server:{} read snapshot db:{}:{}", self.me, db_key, db_value);

                }
                for i in 0..snapshot.latest_requests_key.len() {
                    let mut latest_requests_key: String;
                    let mut latest_requests_value: LatestReply;
                    match labcodec::decode(&snapshot.latest_requests_key[i].clone()) {
                        Ok(key) => {
                            let key: String = key;
                            latest_requests_key = key.clone();
                        },
                        Err(e) => {
                            panic!("{:?}", e);
                        },
                    }
                    match labcodec::decode(&snapshot.latest_requests_value[i].clone()) {
                        Ok(value) => {
                            let value: LatestReply = value;
                            latest_requests_value = value.clone();
                        },
                        Err(e) => {
                            panic!("{:?}", e);
                        },
                    }
                    self.latest_requests.insert(latest_requests_key.clone(), latest_requests_value.clone());
                    kv_debug!("server:{} read snapshot requests:{}:{:?}", self.me, latest_requests_key, latest_requests_value);
                }
            },
            Err(e) => {
                panic!("{:?}", e);
            },
        }
    }
    pub fn save_snapshot(&self) {  //kvserver保存snapshot
        let data = self.creat_snapshot();
        self.rf.save_state_and_snapshot(data);
    }
    pub fn if_need_let_raft_compress_log(&self) -> bool {  //是否要让raft检测需要压缩日志
        if self.maxraftstate.is_some() {
            if self.maxraftstate.unwrap() > 0 {
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
    apply_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    shutdown: Arc<Mutex<bool>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let node = Node {
            me: kv.me,
            server: Arc::new(Mutex::new(kv)),
            apply_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(Mutex::new(false))
        };
        let inode = node.clone();
        /*let apply_ch = node.server.lock().unwrap().apply_ch.clone();
        apply_ch.for_each(move |cmd: raft::ApplyMsg| {
            kv_debug!("cmd:{:?}", cmd);
        })
        .map_err(move |e| println!("error: apply stopped: {:?}", e));*/
        let apply_thread = thread::spawn(move || {
            //let now = Instant::now();
            loop {
                if *inode.shutdown.lock().unwrap() == true {
                    kv_debug!("server:{} shutdown apply",inode.get_id());
                    break;
                }
                //let time2 = now.elapsed().as_millis();
                //println!("server:{} time2:{}", inode.get_id(), time2);
                if let Ok(Async::Ready(Some(apply_msg))) =
                futures::executor::spawn(futures::lazy(|| {
                    inode.server.lock().unwrap().apply_ch.poll()
                }))
                    .wait_future()
                {
                    if !apply_msg.command_valid {
                        continue;
                    }
                    let mut server = inode.server.lock().unwrap();
                    if apply_msg.is_snapshot {  //说明是snapshot应用
                        if server.snapshot_index < apply_msg.command_index{  //接收到更新的snapshot_index
                            server.read_snapshot(apply_msg.snapshot.clone());
                        }
                        continue;
                    }
                    if apply_msg.command.is_empty() || apply_msg.command_index <= server.snapshot_index {
                        continue;   //如果index比snapshot_index更小，不允许重复apply
                    }
                    let command_index = apply_msg.command_index;
                    let mut _entr: OpEntry;
                    match labcodec::decode(&apply_msg.command) {
                        Ok(en) => {
                            _entr = en;
                        }
                        Err(e) => {
                            kv_debug!("error:decode error");
                            continue;
                        }
                    }
                    kv_debug!(
                        "server:{} command_index:{} {:?}",
                        inode.get_id(),
                        command_index,
                        _entr
                    );
                    if server.latest_requests.get(&_entr.client_name).is_none()
                        || server.latest_requests.get(&_entr.client_name).unwrap().seq < _entr.seq
                    {
                        //可更新
                        let mut lre = LatestReply {
                            seq: _entr.seq,
                            value: String::new(),
                        };
                        match _entr.op {
                            1 => {
                                //get
                                if server.db.get(&_entr.key).is_some() {
                                    lre.value = server.db.get(&_entr.key).unwrap().clone();
                                }
                                server.latest_requests.insert(_entr.client_name.clone(), lre.clone());
                                kv_debug!("server:{} client:{} apply:{:?}", server.me, _entr.client_name, lre);
                            }
                            2 => {
                                //put
                                server.db.insert(_entr.key.clone(), _entr.value.clone());
                                server.latest_requests.insert(_entr.client_name.clone(), lre.clone());
                                kv_debug!("server:{} client:{} apply:{:?}", server.me, _entr.client_name, lre);
                            }
                            3 => {
                                //append
                                if server.db.get(&_entr.key).is_some() {
                                    server.db.get_mut(&_entr.key).unwrap().push_str(&_entr.value.clone());
                                } else {
                                    server.db.insert(_entr.key.clone(), _entr.value.clone());
                                }
                                server.latest_requests.insert(_entr.client_name.clone(), lre.clone());
                                kv_debug!("server:{} client:{} apply:{:?}", server.me, _entr.client_name, lre);
                            }
                            _ => {
                                //error
                                kv_debug!("error:apply op error");
                                continue;
                            }
                        }
                    }
                    server.snapshot_index = command_index;
                    server.save_snapshot();   //kv_server借助raft存储snapshot
                    if server.if_need_let_raft_compress_log() {  //是否让raft检测日志压缩
                        //这里由于persister在raft里面,所以无法在kv_server处检测大小，只能传参让raft自己检测并压缩；
                        //也无法在apply_msg中附带参数raftstate的大小，因为若raft一次可能apply多个日志，
                        //则由于锁的原因，apply_msg参数中raftstate的大小不是最新，
                        //所以交给raft处理吧。
                        let maxraftstate: usize = server.maxraftstate.unwrap();
                        server.rf.check_and_do_compress_log(maxraftstate, command_index);

                    }


                    /*if let Some(client_name) = server.ack.get(&command_index) {
                        //需要校验
                        /*if *client_name != _entr.client_name {
                            server.latest_requests.get_mut(&client_name.clone()).unwrap().result = -1;
                        }*/

                        server.ack.remove(&command_index);
                    }*/
                }
            }
            /*while let Ok(Async::Ready(Some(apply_msg))) =
                futures::executor::spawn(futures::lazy(|| inode.server.lock().unwrap().apply_ch.poll())).wait_future() {
                    kv_debug!("cmd:{:?}", apply_msg);
            }*/
        });
        *node.apply_thread.lock().unwrap() = Some(apply_thread);
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
        let apply_thread = self.apply_thread.lock().unwrap().take();
        if apply_thread.is_some() {
            let _ = apply_thread.unwrap().join();
        }
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
        kv_debug!("server:{} get:{:?}", self.get_id(), arg);
        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        //let (send, recv) = mpsc::channel();
        //let index;
        //let term;
        {
            //锁住
            //kv_debug!("server:{} get for lock()", self.get_id());
            let server = self.server.lock().unwrap();
            //kv_debug!("server:{} get lock()", self.get_id());
            //thread::sleep(Duration::from_millis(300));
            if let Some(re) = server.latest_requests.get(&arg.client_name)  {
                //该客户端的回复
                if arg.seq < re.seq {
                    reply.wrong_leader = true;
                    reply.err = String::from("old seq");
                    kv_debug!("kverror:server:{}:{} client:{}:{} get reply:{:?}", self.get_id(), re.seq, arg.client_name, arg.seq, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }
                else if arg.seq == re.seq {  //可直接返回结果
                    reply.wrong_leader = false;
                    reply.err = String::from("OK");
                    reply.value = re.value.clone();
                    kv_debug!("server:{} client:{} get reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }

            }
            let cmd = OpEntry {
                seq: arg.seq,
                client_name: arg.client_name.clone(),
                op: 1,
                key: arg.key.clone(),
                value: String::new(),
            };
            match server.rf.start(&cmd) {
                //发送log
                Ok((index1, term1)) => {  //发送成功
                    //index = index1;
                    //term = term1;
                    //server.ack.entry(index).or_insert(send);
                    //server.ack.insert(index1, (arg.client_name.clone(), arg.seq));
                    reply.wrong_leader = false;
                    kv_debug!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
                    //kv_debug!("server:{} client:{} get start reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));

                }
                Err(_) => {
                    reply.wrong_leader = true;
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            //kv_debug!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
        }
        //kv_debug!("server:{} get unlock()", self.get_id());
        /*if recv
            .recv_timeout(Duration::from_secs(WAIT_CHECK_TIME))
            .is_ok()
        {
            //收到回复
            {
                //锁
                kv_debug!("server:{} recv get lock()", self.get_id());
                let mut server = self.server.lock().unwrap();
                if let Some(re) = server.latest_requests.get(&arg.client_name) {
                    //该客户端的回复
                    if arg.seq == re.seq {
                        //
                        reply.wrong_leader = false;
                        reply.err = String::from("OK");
                        reply.value = re.value.clone();
                        server.ack.remove(&index);
                        kv_debug!("server:{} get reply:{:?}", self.get_id(), reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    } else {
                        kv_debug!(
                            "kverror:server:{} arg.seq:{} re.seq:{}",
                            server.me,
                            arg.seq,
                            re.seq
                        );
                    }
                }
            }
        } else {
            //超时
            kv_debug!("kverror:time out arg:{:?}", arg);
            let mut server = self.server.lock().unwrap();
            server.ack.remove(&index);
        }*/
        //reply.wrong_leader = true;
        //reply.err = String::from("");
        //return Box::new(futures::future::result(Ok(reply)));
        //unimplemented!()
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        kv_debug!("server:{} putappend:{:?}", self.get_id(), arg);
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        //let (send, recv) = mpsc::channel();
        //let index;
        //let term;
        {
            //锁住
            //kv_debug!("server:{} putappend for lock()", self.get_id());
            let server = self.server.lock().unwrap();
            //kv_debug!("server:{} putappend lock()", self.get_id());
            //thread::sleep(Duration::from_millis(300));
            if let Some(re) = server.latest_requests.get(&arg.client_name) {
                //该客户端的回复
                if arg.seq < re.seq {
                    reply.wrong_leader = true;
                    reply.err = String::from("OK");
                    kv_debug!("kverror:server:{}:{} client:{}:{} putappend reply:{:?}", self.get_id(), re.seq, arg.client_name, arg.seq, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }
                else if arg.seq == re.seq {
                    reply.wrong_leader = false;
                    reply.err = String::from("OK");
                    kv_debug!("server:{} client:{} putappend reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));
                }

            }
            let cmd = OpEntry {
                seq: arg.seq,
                client_name: arg.client_name.clone(),
                op: (arg.op + 1) as u64,
                key: arg.key.clone(),
                value: arg.value.clone(),
            };
            match server.rf.start(&cmd) {
                //发送log
                Ok((index1, term1)) => {
                    //index = index1;
                    //term = term1;
                    //server.ack.entry(index).or_insert(send);
                    //server.ack.insert(index, send);
                    //server.ack.insert(index1, (arg.client_name.clone(), arg.seq));
                    reply.wrong_leader = false;
                    kv_debug!("server:{} client:{} start:{:?}", self.get_id(), arg.client_name, cmd);
                    //kv_debug!("server:{} client:{} get start reply:{:?}", self.get_id(), arg.client_name, reply);
                    return Box::new(futures::future::result(Ok(reply)));

                }
                Err(_) => {
                    reply.wrong_leader = true;
                    reply.err = String::from("");
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            //kv_debug!("server:{} start:{:?}", self.get_id(), cmd);
        }
        //kv_debug!("server:{} putappend unlock()", self.get_id());
        /*if recv.recv_timeout(Duration::from_secs(WAIT_CHECK_TIME)).is_ok()
        {
            //收到回复
            {
                //锁
                kv_debug!("server:{} recv putappend lock()", self.get_id());
                let mut server = self.server.lock().unwrap();
                if let Some(re) = server.latest_requests.get(&arg.client_name) {
                    //该客户端的回复
                    if arg.seq == re.seq {
                        //
                        reply.wrong_leader = false;
                        reply.err = String::from("OK");
                        server.ack.remove(&index);
                        kv_debug!("server:{} putappend reply:{:?}", self.get_id(), reply);
                        return Box::new(futures::future::result(Ok(reply)));
                    } else {
                        kv_debug!(
                            "kverror:server:{} arg.seq:{} re.seq:{}",
                            server.me,
                            arg.seq,
                            re.seq
                        );
                    }
                }
            }
        } else {
            //超时
            kv_debug!("kverror:time out arg:{:?}", arg);
            let mut server = self.server.lock().unwrap();
            server.ack.remove(&index);
        }*/
        //reply.wrong_leader = true;
        //reply.err = String::from("");
        //return Box::new(futures::future::result(Ok(reply)));

        //unimplemented!()
    }
}