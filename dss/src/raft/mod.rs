use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{channel, Sender},
    Arc, Mutex,
};
use std::{thread, time};

use rand::Rng;

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labcodec;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

macro_rules! myprintln {
    ($($arg: tt)*) => {
        println!("Debug({}:{}): {}", file!(), line!(),
            format_args!($($arg)*));
    };
}

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Clone, Message)]
pub struct PersistInfo {
    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(int32, tag = "2")]
    pub vote_for: i32,

    #[prost(bytes, repeated, tag = "3")]
    pub log: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(uint64, tag = "2")]
    pub index: u64,

    #[prost(bytes, tag = "3")]
    pub entry: Vec<u8>,
}

impl LogEntry {
    fn new() -> Self {
        LogEntry {
            term: 0,
            index: 0,
            entry: vec![],
        }
    }

    fn from_data(term: u64, index: u64, src_entry: &Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            entry: src_entry.clone(),
        }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    // 连接其他的raft peer
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    // 用于持久化数据
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,

    apply_ch: UnboundedSender<ApplyMsg>,

    vote_for: i32,

    log: Vec<LogEntry>,

    last_index: u64,

    commit_index: u64,

    last_applied: u64,

    // volatile on leader
    //index of next entry to send to that server
    next_index: Vec<u64>,
    // highest log enrty replicated in that server
    match_index: Vec<u64>,

    leader_id: usize,
    // 状态
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,
    //vote_count: Arc<AtomicUsize>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        // 获取持久化的raft状态
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let peer_len = peers.len();
        let mut rf = Raft {
            peers,
            persister,
            me,
            apply_ch,
            vote_for: -1,
            log: vec![LogEntry::new()],
            last_index: 0,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; peer_len],
            match_index: vec![0; peer_len],
            leader_id: 0,
            state: Arc::default(),
            role: Role::FOLLOWER,
            //vote_count: Arc::new(AtomicUsize::new()),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    /// 存储持久化的信息
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        //labcodec::encode(&self.xxx, &mut data).unwrap();
        //labcodec::encode(&self.yyy, &mut data).unwrap();
        let mut raw_data: Vec<u8> = Vec::new();
        let mut pinfo = PersistInfo {
            term: self.state.term,
            vote_for: self.vote_for,
            log: Vec::new(),
        };
        myprintln!("persist {}", self.me);
        for i in 1..self.log.len() {
            myprintln!("persist log[{}]", i);
            let mut raw_log_entry: Vec<u8> = Vec::new();
            labcodec::encode(&self.log[i], &mut raw_log_entry).unwrap();
            pinfo.log.push(raw_log_entry);
        }
        labcodec::encode(&pinfo, &mut raw_data).unwrap();
        //myprintln!("save {} bytes data", raw_data.len());
        self.persister.save_raft_state(raw_data);
    }

    /// restore previously persisted state.
    /// 重新获取之前保存的持久化信息
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            myprintln!("raft data is empty");
            return;
        }
        myprintln!("recover {}", self.me);

        let raw_data: PersistInfo = labcodec::decode(data).unwrap();
        self.update_term_to(raw_data.term);
        for log in &raw_data.log {
            let entry: LogEntry = labcodec::decode(&log).unwrap();
            myprintln!("recover log[{}]", entry.index);
            self.log.push(entry);
            self.last_index += 1;
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns OK(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&mut self, server: usize, args: &RequestVoteArgs, sender: Sender<i32>)-> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        peer.request_vote(&args).map_err(Error::Rpc).wait()
        /*let (tx, rx) = channel();
        peer.spawn(
             peer.request_vote(&args)
                 .map_err(Error::Rpc)
                 .then(move |reply| {
                     match reply{
                         Ok(reply) => {
                             if reply.term > self.state.term() {
                                 myprintln!("get a larger term {} form vote reply and update from {}", reply.term, self.state.term());
                                 self.update_term_to(reply.term);
                             } else if reply.vote_granted {
                                 if self.role == Role::LEADER {
                                     return;
                                 }
                                 self.vote_count.store(vote_count.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
                                 myprintln!("{} got {} votes", self.me, self.vote_count.load(Ordering::SeqCst));
                                 if self.vote_count.load(Ordering::SeqCst) > peer_num / 2 {
                                     myprintln!("receive majority vote {} and become leader", self.vote_count.load(Ordering::SeqCst));
                                     sender.send(1).unwrap();
                                 }
                             }
                         }
                         Err(e) => {
                             myprintln!("failed to get vote result because {:?}", e);
                         }
                     }
                     //tx.send(res);
                     tx.send(1);
                     Ok(())
                 }),
         );
         rx.wait();*/
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        let peer = &self.peers[server];
        peer.append_entries(&args).map_err(Error::Rpc).wait()
    }

    // append_entry
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        if self.state.is_leader() {
            myprintln!("start an entry");
            // only leader can serve client
            // calculate index
            let index = self.last_index + 1;
            let term = self.state.term();
            let mut buf = vec![];
            myprintln!("got command {:?}", command);
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // cache this log to log[]
            // construct a log entry
            let log_entry =
                LogEntry::from_data(self.state.term(), (self.log.len() + 1) as u64, &buf);
            // insert to log[]
            self.log.reserve(2);
            self.log.insert((self.last_index + 1) as usize, log_entry);
            self.last_index += 1;
            myprintln!("{} log vec len is {}", self.me(), self.log.len());
            myprintln!("{} last index is {}", self.me(), self.last_index);
            // return Ok
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn update_term_to(&mut self, term: u64) {
        self.vote_for = -1;
        self.state = Arc::new(State {
            term,
            is_leader: false,
        });
        self.transfer_state(Role::FOLLOWER);
        self.persist();
    }

    fn transfer_state(&mut self, new_role: Role) {
        match new_role {
            Role::FOLLOWER => {
                myprintln!("{} transfer to follower, term[{}]", self.me, self.state.term());
                self.role = Role::FOLLOWER;
            }
            Role::CANDIDATE => {
                {
                    myprintln!("{} transfer to candidate", self.me);
                    // vote for self
                    self.vote_for = self.me as i32;
                    self.role = Role::CANDIDATE;
                }
            }
            Role::LEADER => {
                {
                    // become leader
                    myprintln!("{} transfer to leader", self.me);
                    {
                        // update state
                        self.role = Role::LEADER;
                        self.state = Arc::new(State {
                            term: self.state.term(),
                            is_leader: true,
                        });
                        self.leader_id = self.me;
                        self.vote_for = self.me as i32;
                        // self.commit_index = 0; commit index won't change because of becoming leader
                        // self.last_applied = 0;
                        // init next index to send to each server
                        myprintln!("init next index to {}", self.last_index + 1);
                        self.next_index = vec![self.last_index + 1; self.peer_num() as usize];
                        self.match_index = vec![0; self.peer_num() as usize];
                    }
                }
            }
        }
        self.persist();
    }

    fn update_commit_index(&mut self) {
        let last_index = self.last_index;
        let commit_index = self.commit_index;
        for i in ((commit_index + 1)..=last_index).rev() {
            myprintln!("check for index {}", i);
            let mut matched = 1;
            for match_i in &self.match_index {
                myprintln!("match i = {}", match_i);
                if match_i >= &i {
                    matched += 1;
                }
            }
            if matched > self.peer_num() / 2 {
                myprintln!("update commit index to {}", i);
                self.commit_index = i;
                break;
            }
        }
    }


    fn apply_log(&mut self) {
        if self.commit_index > self.last_applied {
            self.last_applied += 1;
            myprintln!("{} apply a log, log len[{}], commit_index[{}], last_applied[{}]", self.me, self.log.len(), self.commit_index, self.last_applied);
            let msg = ApplyMsg {
                command_valid: true,
                command: self.log[self.last_applied as usize]
                    .entry
                    .clone(),
                command_index: self.last_applied as u64,
            };
            let tx = self.apply_ch.clone();
            thread::spawn(move || {
                tx.send(msg)
                    .map(move |_| println!("apply a log index"))
                    .map_err(|e| eprintln!("error = {:?}", e))
                    .unwrap();
            });
            self.persist();
        }
    }

    fn peer_num(&self) -> u64 {
        self.peers.len() as u64
    }

    fn me(&self) -> u64 {
        self.me as u64
    }

    fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    fn last_log_index(&self) -> u64 {
        self.last_index
    }

    fn last_log_term(&self) -> u64 {
        self.log[self.last_index as usize].term
    }

    fn get_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.state.term(),
            candidate_id: self.me as u64,
            last_log_index: self.last_index,
            last_log_term: self.log[self.last_index as usize].term,
        }
    }

    fn get_append_args(&self, i: u64) -> AppendEntriesArgs {
        let index = self.next_index(i as usize);
        let mut args = AppendEntriesArgs {
            term: self.state.term(),
            leader_id: self.me as u64,
            prev_log_index: index - 1, // prev log index is the prev index of next_index[i]
            prev_log_term: self.log[(index - 1) as usize].term, // prev log index is the prev index of next_index[i],
            entries: vec![],
            entry_term: 0,
            leader_commit: self.commit_index as u64, // index of leader has commited
        };
        if index == self.last_index + 1 {
            // next index of this server is the newest, then send heartbeat
            myprintln!("log is newest {} start heartbeat", self.me);
            args.entries = vec![];
            args.entry_term = args.term;
        } else {
            // or send the corresponding entry
            myprintln!("{} start append entry[{}]", self.me, index);
            args.entries = self.log[index as usize].entry.clone();
            args.entry_term = self.log[index as usize].term;
        }
        args
    }

    fn set_next_index(&mut self, i: usize, index: u64) {
        self.next_index[i] = index;
    }

    fn set_match_index(&mut self, i: usize, index: u64) {
        self.match_index[i] = index;
    }

    fn next_index(&self, i: usize) -> u64 {
        self.next_index[i]
    }

    fn match_index(&self, i: usize) -> u64 {
        self.match_index[i]
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
// 通过RPC实现则使用Raft结构，也可以通过多线程+channel的形式实现（当然是选择RPC）
#[derive(Copy, Clone, PartialEq, Debug)]
enum Role {
    FOLLOWER = 1,
    CANDIDATE = 2,
    LEADER = 3,
}

#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
    timeout_tx: Sender<u32>,
    shutdown: Arc<AtomicBool>,
    // Your code here.
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (tx, rx) = channel();
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            timeout_tx: tx,
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        //spawn a thread to start a election periodly
        let shutdown = node.shutdown.clone();
        let raft = node.raft.clone();
        thread::spawn(move || {
            loop {
                let role;
                {
                    role = raft.lock().unwrap().role;
                }
                let me;
                {
                    me = raft.lock().unwrap().me;
                }
                myprintln!("start a new loop");
                match role {
                    Role::FOLLOWER => {
                        // start a timeout and if time out then become candidate
                        let rand_time = rand::thread_rng().gen_range(200, 500);
                        myprintln!("{} rand sleep {}", me, rand_time);
                        match rx.recv_timeout(time::Duration::from_millis(rand_time)) {
                            Ok(_) => {
                                myprintln!("{} refresh timeout", me);
                            }
                            // wait util timeout and transfer to candidate
                            Err(e) => {
                                myprintln!("{} transfer to CANDIDATE becaues {:?}", me, e);
                                raft.lock().unwrap().transfer_state(Role::CANDIDATE);
                            }
                        }
                    }
                    Role::CANDIDATE => {
                        // start an election and request for vote
                        let (tx, rx) = channel();
                        Node::start_election(raft.clone(), tx.clone());
                        match rx.recv_timeout(time::Duration::from_millis(rand::thread_rng().gen_range(200, 500))) {
                            Ok(_) => {
                                raft.lock().unwrap().transfer_state(Role::LEADER);
                            }
                            Err(e) => {
                                myprintln!("restart because {:?}", e);
                            }
                        }
                    }
                    Role::LEADER => {
                        //myprintln!("{} start heartbeat", me);
                        thread::sleep(time::Duration::from_millis(50));
                        if raft.lock().unwrap().role == Role::LEADER {
                            Node::start_append_entry(raft.clone());
                        }

                    }
                }
                raft.lock().unwrap().apply_log();
                if shutdown.load(Ordering::SeqCst) {
                    myprintln!("exit main loop");
                    break;
                }
            }
        });
        node
    }


    fn start_election(raft: Arc<Mutex<Raft>>, sender: Sender<i32>) {
        // call request_vote for every sever
        let peer_num;
        let me;
        let peers: Vec<RaftClient>;
        let mut vote_arg: RequestVoteArgs;
        {
            let mut raft = raft.lock().unwrap();
            peer_num = raft.peers.len();
            me = raft.me;
            peers = raft.peers.clone();
            // increment term
            let term = raft.state.term();
            raft.update_term_to(term + 1);
            vote_arg = raft.get_vote_args();
            myprintln!("{} update term to {}", me, term + 1);
        }
        myprintln!("{} start election", me);
        let vote_count = Arc::new(AtomicUsize::new(1));
        for i in 0..peer_num {
            if i == me as usize {
                continue;
            }
            if raft.lock().unwrap().role != Role::CANDIDATE {
                return;
            }
            {
                let raft = raft.clone();
                let vote_count = vote_count.clone();
                let peer = peers[i].clone();
                let args = vote_arg.clone();
                let sender = sender.clone();
                thread::spawn(move || {
                    //self cannot be involved in thread::spawn!!!!!
                    myprintln!("request {} for vote", i);
                    match peer.request_vote(&args).map_err(Error::Rpc).wait() {
                        Ok(reply) => {
                            let mut raft = raft.lock().unwrap();
                            if reply.term > raft.state.term() {
                                myprintln!("get a larger term {} form vote reply and update from {}", reply.term, raft.state.term());
                                raft.update_term_to(reply.term);
                                raft.transfer_state(Role::FOLLOWER);
                            } else if reply.vote_granted {
                                if raft.role == Role::LEADER {
                                    return;
                                }
                                vote_count.store(vote_count.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
                                myprintln!("{} got {} votes", me, vote_count.load(Ordering::SeqCst));
                                if vote_count.load(Ordering::SeqCst) > peer_num / 2 {
                                    myprintln!("receive majority vote {} and become leader", vote_count.load(Ordering::SeqCst));
                                    sender.send(1).unwrap();
                                }
                            }
                        }
                        Err(e) => {
                            myprintln!("failed to get vote result because {:?}", e);
                        }
                    }
                    //vote_req.store(vote_count.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
                });
            }
        }
        // each call is a thread
        // thread communicate by channel
    }

    fn start_append_entry(raft: Arc<Mutex<Raft>>) {
        let me;
        let term;
        let peers: Vec<RaftClient>;
        let peer_num;
        {
            let raft = raft.lock().unwrap();
            me = raft.me;
            term = raft.state.term();
            peers = raft.peers.clone();
            peer_num = raft.peer_num();
        }
        myprintln!("{} start append entry call, term[{}]", me, term);
        for i in 0..peer_num {
            if i == me as u64 {
                continue;
            }
            {
                // if this peer is not leader anymore stop this append
                if raft.lock().unwrap().role != Role::LEADER {
                    return;
                }
            }
            let mut append_args: AppendEntriesArgs;
            {
                let raft = raft.lock().unwrap();
                append_args = raft.get_append_args(i);
            }

            let peer = peers[i as usize].clone();
            let raft = raft.clone();
            thread::spawn(move || {
                match peer.append_entries(&append_args).wait().map_err(Error::Rpc) {
                    Ok(reply) => {
                        if reply.success {
                            // majority of server has reply success
                            myprintln!("receive success from {}", i);
                            let mut raft = raft.lock().unwrap();
                            let next = raft.next_index(i as usize);
                            if append_args.entries.len() > 0 {
                                raft.set_next_index(i as usize, next + 1);
                                raft.set_match_index(i as usize, next);
                                raft.update_commit_index();
                                myprintln!("{} update next index to {}", i, next + 1);
                                myprintln!("{} update match index to {}", i, next);
                            }
                        } else {
                            // append false
                            if raft.lock().unwrap().role != Role::LEADER {
                                myprintln!("{} not a leader any more and quit directly", me);
                                return;
                            }
                            if reply.term > append_args.term {
                                myprintln!("{} become follower because term", me);
                                let mut raft = raft.lock().unwrap();
                                raft.update_term_to(reply.term);
                                raft.transfer_state(Role::FOLLOWER);
                            } else {
                                let mut raft = raft.lock().unwrap();
                                let next = raft.next_index(i as usize);
                                if next != 0 {
                                    raft.set_next_index(i as usize, next - 1);
                                    //raft.set_match_index(i as usize, next);
                                    myprintln!("{} rollback next index to {}", i, next -1);
                                }
                            }
                        }
                        futures::future::result(Ok(()))
                    },
                    Err(e) => {
                        myprintln!("append failed because {:?}", e);
                        futures::future::result(Err(e))
                    }
                };
            });
            myprintln!("{} end this heartbeat", me);
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns false. otherwise start the
    /// agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first return value is the index that the command will appear at
    /// if it's ever committed. the second return value is the current
    /// term. the third return value is true if this server believes it is
    /// the leader.
    /// (log index, term, is_leader)
    /// 用于发起一个command,append_entry
    /// This method must return quickly.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        // unimplemented!()
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // unimplemented!()
        self.raft.lock().unwrap().state.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        self.raft.lock().unwrap().state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        myprintln!("kill {}", self.raft.lock().unwrap().me);
        self.raft.lock().unwrap().persist();
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let me;
        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };
        {
            let raft = self.raft.lock().unwrap();
            me = raft.me;
            reply.term = raft.state.term();
        }
        self.timeout_tx.send(1).unwrap();
        // reset folower timeout

        let mut raft = self.raft.lock().unwrap();
        let term = raft.state.term();
        reply.term = term;
        myprintln!(
                "{}[term {}] get vote req from {}[term {}]",
                me,
                term,
                args.candidate_id,
                args.term
            );
        let vote_for = raft.vote_for;
        if term > args.term {
            // term is larger than args
            myprintln!("{} reply {} because larger term {} > {}", me, reply.vote_granted, term, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        } else if term < args.term {
            // term is less smaller than args
            raft.update_term_to(args.term);
            raft.transfer_state(Role::FOLLOWER);
            raft.vote_for = -1;
        }

        let mut up_to_date = true;

        if raft.last_log_term() > args.last_log_term {
            up_to_date = false;
            myprintln!("{} reject vote because larger last log term {} > {}", me, raft.last_log_term(), args.last_log_term);
        } else if raft.last_log_term() == args.last_log_term {
            if raft.last_log_index() > args.last_log_index {
                up_to_date = false;
                myprintln!("{} reject vote because larger last log index {} > {}", me, raft.last_log_index(), args.last_log_index);
            }
        }

        if (raft.vote_for == -1 || raft.vote_for == args.candidate_id as i32) && up_to_date {
            myprintln!("{} granted vote for {}", me, args.candidate_id);
            reply.vote_granted = true;
        }

        myprintln!("{} reply {}", me, reply.vote_granted);
        Box::new(futures::future::result(Ok(reply)))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // reset timeout
        self.timeout_tx.send(1).unwrap();
        let me;
        let term;
        let role;
        let last_log_index;
        let last_log_term;
        {
            let mut raft = self.raft.lock().unwrap();
            me = raft.me;
            term = raft.state.term();
            role = raft.role;
            last_log_index = raft.last_index;
            myprintln!("{} get last index {}, log len is {}", me, last_log_index, raft.log.len());
            last_log_term = raft.log[last_log_index as usize].term;
            if raft.leader_id != args.leader_id as usize {
                // update leader id
                raft.leader_id = args.leader_id as usize
            };
        }
        if args.entries.len() == 0 {
            myprintln!("{} get heartbeat from {}", me, args.leader_id);
        } else {
            myprintln!("{} get log from {}", me, args.leader_id);
        }
        myprintln!("{} got last_index[{}] and last term [{}]", me, last_log_index, last_log_term);
        let mut reply = AppendEntriesReply {
            term,
            success: false,
        };
        if args.term < reply.term {
            // false
            myprintln!("{} reply false for {} because term {} > {}", me, args.leader_id, reply.term, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        } else if args.term > reply.term {
            let mut raft = self.raft.lock().unwrap();
            raft.update_term_to(args.term);
            if role != Role::FOLLOWER {
                raft.transfer_state(Role::FOLLOWER);
            }
        } else {
            reply.success = true;
        }
        // do log consistency check
        if last_log_index < args.prev_log_index {
            //log不够，返回并next_index -1重来
            myprintln!(
                    "{} reply false for {} because log lost:[index:{}-{}]",
                    me,
                    args.leader_id,
                    last_log_index,
                    args.prev_log_index,
                );
            reply.success = false;
            return Box::new(futures::future::result(Ok(reply)));
        }
        // 一定是last_log_index大于等于prev_log_index的情况
        let prev_log_term;
        {
            let raft = self.raft.lock().unwrap();
            prev_log_term = raft.log[args.prev_log_index as usize].term;
        }
        if prev_log_term != args.prev_log_term {
            // prev_log_term不匹配，回退重试
            myprintln!(
                    "{} reply false for {} because unmatched term:[term: {}-{}]",
                    me,
                    args.leader_id,
                    prev_log_term,
                    args.prev_log_term
                );
            reply.success = false;
            return Box::new(futures::future::result(Ok(reply)));
        }
        {
            //do log replicate
            let mut raft = self.raft.lock().unwrap();
            if raft.log.len() > (args.prev_log_index + 1) as usize {
                // delete wrong log
                for i in args.prev_log_index + 1..(raft.log.len() as u64) {
                    myprintln!("{} remove log in {}", me, i);
                    raft.log.remove(args.prev_log_index as usize);
                    raft.last_index -= 1;
                }
            }
            let pos = args.prev_log_index + 1;
            if args.entries.len() != 0 {
                myprintln!(
                "{} append log, last index[{}], log len[{}], commit_index[{}]",
                    me,
                    raft.last_index,
                    raft.log.len(),
                    raft.commit_index
                );
                raft.log.reserve(1);
                raft.log.append(&mut vec![LogEntry::from_data(args.entry_term, pos + 1, &args.entries)]);
                raft.last_index = args.prev_log_index + 1;
                myprintln!("{} last index update to prev_log_index[{}] + 1", me, args.prev_log_index);
            }
            reply.success = true;

            if args.leader_commit > raft.commit_index as u64 {
                raft.commit_index = raft.commit_index + 1;
                myprintln!("update commit index to {}", raft.commit_index);
            }
            reply.success = true;
            myprintln!("reply heartbeat success");
            return Box::new(futures::future::result(Ok(reply)));
        }
    }
}
