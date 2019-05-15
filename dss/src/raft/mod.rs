use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{channel, Sender},
    Arc, Mutex,
};
use std::thread;
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
use std::time::Duration;
use std::sync::mpsc::Receiver;
use std::any::TypeId;

use crate::kvraft::server::Operation as Entry;

macro_rules! myprintln {
    ($($arg: tt)*) => {
        //println!("Debug({}:{}): {}", file!(), line!(),
          //  format_args!($($arg)*));
    };
}

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub is_snapshot: bool,
    pub snapshot: Vec<u8>,
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

    #[prost(uint64, tag = "4")]
    pub snapshot_index: u64,

    #[prost(uint64, tag = "5")]
    pub snapshot_term: u64,
}


#[derive(Copy, Clone, PartialEq, Debug)]
enum Role {
    FOLLOWER = 1,
    CANDIDATE = 2,
    LEADER = 3,
}

/*enum Response {
    Timeout,
    RefreshTimeout,
}*/

const MAX_ELECTION_TIMEOUT: u64 = 500;
const MIN_ELECTION_TIMEOUT: u64 = 300;
const HEARTBEAT_TIMEOUT: u64 = 50;

struct RaftSnapshot {
    me: usize,

    vote_for: i32,

    log: Vec<LogEntry>,

    last_index: u64,

    commit_index: u64,

    last_applied: u64,

    next_index: Vec<u64>,
    match_index: Vec<u64>,

    leader_id: usize,
    term: u64,
    role: Role,
    peer_num: u64,

    snapshot_index: u64,
    snapshot_term: u64,
}

impl RaftSnapshot {
    fn get_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.term,
            candidate_id: self.me as u64,
            last_log_index: self.last_index,
            last_log_term: self.log[self.last_index as usize].term,
        }
    }

    fn get_append_args(&self, i: u64) -> AppendEntriesArgs {
        let mut index = self.next_index[i as usize] as usize;
        myprintln!("get index@{}, log len is {}", index, self.log.len());
        let mut args = AppendEntriesArgs {
            term: self.term,
            leader_id: self.me as u64,
            prev_log_index: index as u64 - 1, // prev log index is the prev index of next_index[i]
            prev_log_term: self.log[(index - 1) as usize].term, // prev log index is the prev index of next_index[i],
            entries: vec![],
            entry_term: 0,
            leader_commit: self.commit_index as u64, // index of leader has commited
        };
        while index < self.log.len() {
            myprintln!("add a log@{}", index);
            args.entries.push(self.log[index].clone());
            index += 1;
        }
        /*if index == self.last_index + 1 {
            // next index of this server is the newest, then send heartbeat
            myprintln!("prepare heartbeat to [{}]", i);
            args.entries = vec![];
            args.entry_term = args.term;
        } else {
            // or send the corresponding entry
            myprintln!("prepare append entry to [{}]@{}", i, index);
            args.entries = self.log[index as usize].entry.clone();
            args.entry_term = self.log[index as usize].term;
        }*/
        args
    }

    fn last_log_index(&self) -> u64 {
        (self.snapshot_index as usize + self.log.len() - 1) as u64
    }

    fn last_log_term(&self) -> u64 {
        self.log[(self.last_log_index() - self.snapshot_index) as usize].term
    }

    fn get_log(&self, index: u64) -> Option<LogEntry> {
        let index = index - self.snapshot_index;
        if ((self.log.len() - 1) as u64) < index {
            None
        } else {
            Some(self.log[index as usize].clone())
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
    // state: Arc<State>,
    term: u64,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,
    //vote_count: Arc<AtomicUsize>,

    vote_count: u64,

    snapshot_index: u64,

    snapshot_term: u64,
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
            next_index: vec![1; peer_len],
            match_index: vec![0; peer_len],
            leader_id: peer_len,
            //state: Arc::default(),
            term: 0,
            role: Role::FOLLOWER,
            vote_count: 0,
            snapshot_index: 0,
            snapshot_term: 0,
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
            term: self.term,
            vote_for: self.vote_for,
            log: Vec::new(),
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
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

    pub fn save_state_and_snapshot(&self, snap_data: Vec<u8>) {
        let mut raw_data: Vec<u8> = Vec::new();
        let mut pinfo = PersistInfo {
            term: self.term,
            vote_for: self.vote_for,
            log: Vec::new(),
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
        };
        myprintln!("persist {}", self.me);
        for i in 1..self.log.len() {
            myprintln!("persist log[{}]", i);
            let mut raw_log_entry: Vec<u8> = Vec::new();
            labcodec::encode(&self.log[i], &mut raw_log_entry).unwrap();
            pinfo.log.push(raw_log_entry);
        }
        labcodec::encode(&pinfo, &mut raw_data).unwrap();
        self.persister.save_state_and_snapshot(raw_data, snap_data);
        //my_debug!("id:{} save_state_and_snapshot",self.me);
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
        self.vote_for = raw_data.vote_for;
        self.snapshot_index = raw_data.snapshot_index;
        self.snapshot_term = raw_data.snapshot_term;
        self.term = raw_data.term
    }

    pub fn delete_prev_log(&mut self, save_index: u64) {  //save_index是日志的index
        //删除save_index前面的log，save_index不删除
        let save_index = save_index - self.snapshot_index;  //vector数组的index
        if ((self.log.len() - 1) as u64) < save_index {  //log不够，
            myprintln!("error:id:{} delete prev log save_index:{} snapshot_index:{} log:{}",
                self.me, save_index, self.snapshot_index, self.log.len());
            return;
        }
        let _delete: Vec<LogEntry> = self.log.drain(..(save_index as usize)).collect();
        for de in &_delete {
            let entry = de.entry.clone();
            let mut _entr: Option<Entry> = None;
            match labcodec::decode(&entry) {
                Ok(en) => {
                    _entr = Some(en);
                }
                Err(e) => {}
            }
            myprintln!(
                "id:{} delete prev log:[{}:{}:{:?}]",
                self.me,
                de.index,
                de.term,
                _entr
            );
        }
    }

    pub fn check_and_do_compress_log(&mut self, maxraftstate: usize, index: u64) {
        if maxraftstate > self.persister.raft_state().len() {  //未超过，无需压缩日志
            return;
        }
        if index > self.commit_index || index <= self.snapshot_index {
            myprintln!("error:id:{} compress_log error index:{} commit:{} snapshot:{}",
                self.me, index, self.commit_index, self.snapshot_index);
            return;
        }
        self.delete_prev_log(index);  //删除index之前的日志
        self.snapshot_index = index;
        self.snapshot_term = self.log[0].term;
        self.persist();  //无需保存snapshot，因为kv_server前面保存了
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
    fn send_request_vote(&mut self, raft: Arc<Mutex<Raft>>, server: usize, args: &RequestVoteArgs, sender: Sender<bool>) {
        //let peer = &self.peers[server];
        //peer.request_vote(&args).map_err(Error::Rpc).wait()
        //let (tx, rx) = channel();
        self.peers[server].spawn(
            self.peers[server].request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |reply| {
                    match reply {
                        Ok(reply) => {
                            //let snap = raft.lock().unwrap().snapshot();
                            let mut raft = raft.lock().unwrap();
                            myprintln!("{} get a response", raft.me);
                            if raft.role == Role::LEADER {
                                return Ok(());
                            }
                            if reply.term > raft.term {
                                myprintln!("get a larger term {} form vote reply and update from {}", reply.term, raft.term);

                                raft.update_term_to(reply.term);
                                raft.transfer_state(Role::FOLLOWER);
                            } else if reply.vote_granted {
                                raft.vote_count += 1;
                                myprintln!("{} got {} votes", raft.me, raft.vote_count);
                                if raft.vote_count > (raft.peers.len() / 2) as u64 {
                                    myprintln!("receive majority vote {} and {} become leader", raft.vote_count, raft.me);
                                    sender.send(true).unwrap();
                                    //timer.invalid();
                                }
                            }
                        }
                        Err(e) => {
                            myprintln!("failed to get vote result because {:?}", e);
                        }
                    }
                    Ok(())
                }),
        );
    }

    fn send_append_entries(&mut self, raft: Arc<Mutex<Raft>>, server: usize, args: AppendEntriesArgs) {
        //let peer = &self.peers[server];
        //peer.append_entries(&args).map_err(Error::Rpc).wait()
        self.peers[server].spawn(
            self.peers[server].append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |reply| {
                    match reply {
                        Ok(reply) => {
                            let mut raft = raft.lock().unwrap();
                            let me = raft.me;
                            let i = server;
                            if raft.role != Role::LEADER {
                                myprintln!("{} not a leader any more and quit directly", me);
                                return Ok(());
                            }
                            if reply.success {
                                // majority of server has reply success
                                myprintln!("receive success from {}", i);
                                raft.set_match_index(i as usize, reply.conflict_index);
                                raft.set_next_index(i as usize, reply.conflict_index + 1);
                                /*let next = raft.next_index(i as usize);
                                if entry_num != 0 {
                                    raft.set_next_index(i as usize, next + 1);
                                    raft.set_match_index(i as usize, next);
                                    raft.update_commit_index();
                                    myprintln!("{} update next index to {}, match index to {}", i, next + 1, next);
                                }*/
                            } else {
                                if raft.role == Role::LEADER && reply.term > raft.term {
                                    myprintln!("{} become follower because term", me);
                                    raft.update_term_to(reply.term);
                                    raft.transfer_state(Role::FOLLOWER);
                                    //timer.invalid();
                                    return Ok(());
                                } else {
                                    /*let next = raft.next_index(i as usize);
                                    if next != 0 {
                                        raft.set_next_index(i as usize, next - 1);
                                        //raft.set_match_index(i as usize, next);
                                        myprintln!("{} rollback next index to {}", i, next -1);
                                    }*/
                                    // need to rollback nextindex
                                    let mut find_conflict = false;
                                    let mut last_index: u64 = 0;
                                    if reply.conflict_term != 0 {
                                        for i in (raft.log.len() - 1)..=0 {
                                            if raft.log[i].term == reply.conflict_term {
                                                find_conflict = true;
                                                last_index = i as u64 + raft.snapshot_index;
                                                break;
                                            }
                                        }
                                        if find_conflict {
                                            myprintln!("set next_index[{}] to {}", i, last_index.min(reply.conflict_index));
                                            raft.set_next_index(i, last_index.min(reply.conflict_index));
                                        } else {
                                            myprintln!("set next_index[{}] to {}", i, reply.conflict_index);
                                            raft.set_next_index(i, reply.conflict_index);
                                        }
                                    } else {
                                        myprintln!("set next_index[{}] to {}", i, reply.conflict_index);
                                        raft.set_next_index(i, reply.conflict_index);
                                    }
                                }
                            }
                            //sender.send(Response::AppendResponse(server, args.entries.len() as u64, reply)).unwrap();
                        }
                        Err(e) => {
                            myprintln!("failed to get vote result because {:?}", e);
                        }
                    }
                    Ok(())
                }),
        );
    }

    // append_entry
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        if self.is_leader() {
            myprintln!("start an entry");
            // only leader can serve client
            // calculate index
            let index = self.last_index + 1;
            let term = self.term;
            let mut buf = vec![];
            myprintln!("got command {:?}", command);
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // cache this log to log[]
            // construct a log entry
            let log_entry =
                LogEntry::from_data(self.term, (self.log.len() + 1) as u64, &buf);
            // insert to log[]
            self.log.reserve(2);
            self.log.insert((self.last_index + 1) as usize, log_entry);
            self.last_index += 1;
            myprintln!("{} log vec len is {}", self.me, self.log.len());
            myprintln!("{} last index is {}", self.me, self.last_index);
            // return Ok
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn update_term_to(&mut self, term: u64) {
        self.vote_for = -1;
        self.term = term;
        self.vote_count = 0;
        self.persist();
    }

    fn transfer_state(&mut self, new_role: Role) {
        match new_role {
            Role::FOLLOWER => {
                myprintln!("{} transfer to follower, term[{}]", self.me, self.term);
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
                        /*self.state = Arc::new(State {
                            term: self.state.term(),
                            is_leader: true,
                        });*/
                        self.leader_id = self.me;
                        self.vote_for = self.me as i32;
                        // self.commit_index = 0; commit index won't change because of becoming leader
                        // self.last_applied = 0;
                        // init next index to send to each server
                        myprintln!("init next index to {}", self.last_index + 1);
                        self.next_index = vec![self.last_index + 1; self.peers.len() as usize];
                        self.match_index = vec![0; self.peers.len() as usize];
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
            if matched > self.peers.len() / 2 {
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
                is_snapshot: false,
                snapshot: vec![],
            };
            let tx = self.apply_ch.clone();
            tx.unbounded_send(msg)
                .map(move |_| println!("apply a log index"))
                .map_err(|e| eprintln!("error = {:?}", e))
                .unwrap();
            self.persist();
        }
    }

    fn term(&self) -> u64 {
        self.term
    }

    fn vote_for(&mut self, who: i32) {
        self.vote_for = who;
        if who == self.me as i32 {
            self.vote_count += 1;
        }
    }

    fn is_leader(&self) -> bool {
        self.leader_id == self.me
    }

    fn set_leader(&mut self, who: usize) {
        self.leader_id = who;
    }

    /*fn get_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.term,
            candidate_id: self.me as u64,
            last_log_index: self.last_index,
            last_log_term: self.log[self.last_index as usize].term,
        }
    }*/

    /*fn get_append_args(&self, i: u64) -> AppendEntriesArgs {
        let index = self.next_index(i as usize);
        let mut args = AppendEntriesArgs {
            term: self.term,
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
    }*/

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

    fn snapshot(&self) -> RaftSnapshot {
        RaftSnapshot {
            me: self.me,
            vote_for: self.vote_for,
            log: self.log.clone(),
            last_index: self.last_index,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            next_index: self.next_index.clone(),
            match_index: self.match_index.clone(),
            leader_id: self.leader_id,
            term: self.term,
            role: self.role,
            peer_num: self.peers.len() as u64,
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
        }
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
enum TimeoutType {
    Election,
    HeartBeat,
}

/*truct MyTimer{
    tx: Sender<Response>,
    valid: Arc<AtomicBool>,
}

impl MyTimer{
    fn new(tx: Sender<Response>) -> Self{
        MyTimer{
            tx,
            valid: Arc::new(AtomicBool::new(true)),
        }
    }

    fn time_out(&self, timeout_type: TimeoutType){
        let valid = self.valid.clone();
        let tx = self.tx.clone();
        let time = match timeout_type{
            TimeoutType::Election => rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT),
            TimeoutType::HeartBeat => HEARTBEAT_TIMEOUT,
        };
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(time));
            if valid.load(Ordering::SeqCst){
                tx.send(Response::Timeout).unwrap();
            }
        });
    }

    fn invalid(&mut self){
        self.valid.store(false, Ordering::SeqCst);
    }
}*/

#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
    msg_tx: Sender<bool>,
    shutdown: Arc<AtomicBool>,
    // Your code here.
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (tx, rx) = channel::<bool>();
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            msg_tx: tx.clone(),
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        Node::raft_main(node.clone(), rx, tx.clone());
        node
    }

    fn get_timeout(&self, t: TimeoutType) -> u64 {
        match t {
            TimeoutType::HeartBeat => HEARTBEAT_TIMEOUT,
            TimeoutType::Election => {
                rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
            }
        }
    }

    fn raft_main(node: Node, rx: Receiver<bool>, tx: Sender<bool>) {
        let raft = node.raft.clone();
        let shutdown = node.shutdown.clone();
        //let mut node = node.clone();
        //spawn a thread to start a election periodly
        thread::spawn(move || {
            loop {
                let role;
                let me;
                {
                    role = raft.lock().unwrap().role;
                    me = raft.lock().unwrap().me;
                }
                myprintln!("start a new loop");
                //let mut timer = MyTimer::new(tx.clone());
                match role {
                    Role::FOLLOWER => {
                        // start a timeout and if time out then become candidate
                        // refresh之后旧的timer仍在工作，会传来新的timeout
                        //timer.time_out(TimeoutType::Election);
                        //myprintln!("{} rand sleep {}", me, rand_time);
                        match rx.recv_timeout(Duration::from_millis(node.get_timeout(TimeoutType::Election))) {
                            Ok(_) => (),
                            // wait util timeout and transfer to candidate
                            Err(e) => {
                                myprintln!("{} transfer to CANDIDATE becaues timeout", me);
                                raft.lock().unwrap().transfer_state(Role::CANDIDATE);
                            }
                        }
                    }
                    Role::CANDIDATE => {
                        // start an election and request for vote
                        //let (tx, rx) = channel();
                        node.start_election();
                        match rx.recv_timeout(Duration::from_millis(node.get_timeout(TimeoutType::Election))) {
                            Ok(res) => {
                                // transfer to leader
                                if res {
                                    raft.lock().unwrap().transfer_state(Role::LEADER);
                                }
                            }
                            Err(e) => (),
                        }
                    }
                    Role::LEADER => {
                        thread::sleep(Duration::from_millis(node.get_timeout(TimeoutType::HeartBeat)));
                        if raft.lock().unwrap().role != Role::LEADER {
                            continue;
                        }
                        node.start_append_entry();
                        raft.lock().unwrap().update_commit_index();
                    }
                }
                raft.lock().unwrap().apply_log();
                if shutdown.load(Ordering::SeqCst) {
                    myprintln!("exit main loop");
                    break;
                }
            }
        });
    }

    fn start_election(&self/*, raft: Arc<Mutex<Raft>>, sender: Sender<Response>*/) {
        // call request_vote for every sever
        let raft = self.raft.clone();
        let sender = self.msg_tx.clone();
        let snap = raft.lock().unwrap().snapshot();
        let me = snap.me;
        let term = snap.term;
        raft.lock().unwrap().update_term_to(term + 1);
        raft.lock().unwrap().vote_for(me as i32);

        let snap = raft.lock().unwrap().snapshot();
        let term = snap.term;
        let peer_num = snap.peer_num;
        let vote_arg: RequestVoteArgs = snap.get_vote_args();
        // increment term
        myprintln!("{} start election and update term to {}", me, term);
        for i in 0..peer_num {
            if i == me as u64 {
                continue;
            }
            if raft.lock().unwrap().role != Role::CANDIDATE {
                return;
            }
            let sender = sender.clone();
            raft.lock().unwrap().send_request_vote(raft.clone(), i as usize, &vote_arg, sender);
        }
    }

    /*fn start_election(raft: Arc<Mutex<Raft>>, sender: Sender<i32>) {
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
    }*/

    fn start_append_entry(&self) {
        let raft = self.raft.clone();
        //let sender = self.msg_tx.clone();
        let snap = raft.lock().unwrap().snapshot();
        let me = snap.me;
        let term = snap.term;
        let peer_num = snap.peer_num;
        myprintln!("{} start append entry call, term[{}]", me, term);
        for i in 0..peer_num {
            if i == me as u64 {
                continue;
            }
            // if this peer is not leader anymore stop this append
            if raft.lock().unwrap().role != Role::LEADER {
                return;
            }
            let append_args = snap.get_append_args(i);
            raft.lock().unwrap().send_append_entries(self.raft.clone(), i as usize, append_args);
            myprintln!("{} end this heartbeat", me);
        }
    }

    /*
    fn start_append_entry(raft: Arc<Mutex<Raft>>, sender: Sender<Response>) {
        let me;
        let term;
        let peers: Vec<RaftClient>;
        let peer_num;
        {
            let raft = raft.lock().unwrap();
            me = raft.me;
            term = raft.term;
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
            let append_args: AppendEntriesArgs;
            {
                let raft = raft.lock().unwrap();
                append_args = raft.get_append_args(i);
            }

            let peer = peers[i as usize].clone();
            let raft = raft.clone();
            raft.lock().unwrap().send_append_entries(i as usize, &append_args, sender.clone());
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
                    }
                    Err(e) => {
                        myprintln!("append failed because {:?}", e);
                        futures::future::result(Err(e))
                    }
                };
            });
            myprintln!("{} end this heartbeat", me);
        }
    }
    */

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
        self.raft.lock().unwrap().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        self.raft.lock().unwrap().is_leader()
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

    pub fn save_state_and_snapshot(&self, data: Vec<u8>) {
        self.raft.lock().unwrap().save_state_and_snapshot(data);
    }

    pub fn check_and_do_compress_log(&self, maxraftstate: usize, index: u64) {
        self.raft.lock().unwrap().check_and_do_compress_log(maxraftstate, index);
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let mut snap = self.raft.lock().unwrap().snapshot();
        let me = snap.me;
        myprintln!(
                "{}[term {}] get vote req from {}[term {}]",
                snap.me,
                snap.term,
                args.candidate_id,
                args.term
            );
        if snap.term > args.term {
            // term is larger than args
            let reply = RequestVoteReply {
                term: snap.term,
                vote_granted: false,
            };
            myprintln!("{} reply {} because larger term {} > {}", me, reply.vote_granted, snap.term, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        } else if snap.term < args.term {
            // term is less smaller than args
            self.raft.lock().unwrap().update_term_to(args.term);
            if snap.role != Role::FOLLOWER {
                self.raft.lock().unwrap().transfer_state(Role::FOLLOWER);
            }
            // state change update snapshot
            snap = self.raft.lock().unwrap().snapshot();
        }

        let mut reply = RequestVoteReply {
            term: snap.term,
            vote_granted: false,
        };

        if snap.vote_for == -1 {
            if (args.last_log_term == snap.last_log_term()
                && args.last_log_index >= snap.last_log_index())
                || args.last_log_term > args.last_log_term {
                // reset follower timeout
                self.msg_tx.send(false).unwrap();
                self.raft.lock().unwrap().vote_for(args.candidate_id as i32);
                if snap.role != Role::FOLLOWER { self.raft.lock().unwrap().transfer_state(Role::FOLLOWER); }
                reply.vote_granted = true;
            }
        }

        /*if snap.last_log_term() > args.last_log_term {
            up_to_date = false;
            myprintln!("{} reject vote because larger last log term {} > {}", me, snap.last_log_term(), args.last_log_term);
        } else if snap.last_log_term() == args.last_log_term {
            if snap.last_log_index() > args.last_log_index {
                up_to_date = false;
                myprintln!("{} reject vote because larger last log index {} > {}", me, snap.last_log_index(), args.last_log_index);
            }
        }

        if snap.vote_for == -1 && up_to_date {
            myprintln!("{} granted vote for {}", me, args.candidate_id);
            self.raft.lock().unwrap().vote_for(args.candidate_id as i32);
            reply.vote_granted = true;
        }*/

        myprintln!("{} reply {}", me, reply.vote_granted);
        Box::new(futures::future::result(Ok(reply)))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // reset timeout
        self.msg_tx.send(false).unwrap();
        let mut snap = self.raft.lock().unwrap().snapshot();
        let me = snap.me;
        let term = snap.term;
        let role = snap.role;
        //myprintln!("{} get last index [{}], last term [{}], log len is {}", me, last_log_index, last_log_term, snap.log.len());
        if snap.leader_id != args.leader_id as usize {
            // update leader id
            self.raft.lock().unwrap().set_leader(args.leader_id as usize);
        };

        if args.entries.len() == 0 {
            myprintln!("{} get heartbeat from {}", me, args.leader_id);
        } else {
            myprintln!("{} get log from {}", me, args.leader_id);
        }
        if args.term < snap.term {
            // false
            let reply = AppendEntriesReply {
                term,
                success: false,
                conflict_index: 0,
                conflict_term: 0,
            };
            myprintln!("{} reply false for {} because term {} > {}", me, args.leader_id, reply.term, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        } else if args.term > term {
            myprintln!("{} update term to {}", me, args.term);
            self.raft.lock().unwrap().update_term_to(args.term);
            if role != Role::FOLLOWER {
                self.raft.lock().unwrap().transfer_state(Role::FOLLOWER);
            }
            snap = self.raft.lock().unwrap().snapshot();
            //reply.success = true;
        } else {
            //reply.success = true;
        }

        // check vote for?

        let mut reply = AppendEntriesReply {
            term: snap.term,
            success: false,
            conflict_term: 0,
            conflict_index: 0,
        };

        if args.prev_log_index < snap.snapshot_index {
            reply.success = false;
            reply.term = snap.term;
            reply.conflict_term = snap.snapshot_term;
            reply.conflict_index = snap.snapshot_index;
            return Box::new(futures::future::result(Ok(reply)));
        }

        let (mut prev_log_index, mut prev_log_term) = (0, 0);
        if args.prev_log_index < (snap.log.len() as u64 + snap.snapshot_index) {
            // 正常情况：prev_log_index = log_len - 1
            // 非正常情况： prev_log_index < log_len - 1
            prev_log_index = args.prev_log_index;
            prev_log_term = snap.log[(prev_log_index - snap.snapshot_index) as usize].term;
        }// else follower丢失部分日志
        myprintln!("{} get prev_log_index@{}, prev_log_term[{}]", me, prev_log_index, prev_log_term);
        if prev_log_index == args.prev_log_index && prev_log_term == args.prev_log_term {
            // prev_log_term匹配，从prev_index开始追加日志
            // 同时覆盖不正常的日志
            myprintln!("{} log match and append", me);
            let mut raft = self.raft.lock().unwrap();
            reply.success = true;
            myprintln!("{} log len is {}, entry num is {}", me, raft.log.len(), args.entries.len());
            //myprintln!("truncate {} - {} - 1", len, prev_log_index);
            let snap_index = raft.snapshot_index;
            raft.log.truncate((prev_log_index + 1 - snap_index) as usize);
            raft.log.append(&mut args.entries.clone());
            raft.last_index = raft.snapshot_index + raft.log.len() as u64 - 1;
            if args.leader_commit > raft.commit_index {
                // update commit index of raft
                raft.commit_index = args.leader_commit.min(raft.last_index);
                myprintln!("{} update commit index to {}", me, raft.commit_index);
            }
            reply.conflict_term = raft.log[(raft.last_index - raft.snapshot_index) as usize].term;
            reply.conflict_index = raft.last_index;
        } else {
            // prev_log_term不匹配
            reply.success = false;
            let mut start = 1 + snap.snapshot_index;
            reply.conflict_term = prev_log_term;
            if reply.conflict_term == 0 {
                // follower has less log than leader
                start = snap.log.len() as u64 + snap.snapshot_index;
                reply.conflict_term = snap.log[(start - snap.snapshot_index) as usize - 1].term;
                myprintln!("log is lost and start@[{}], term is {}", start, reply.conflict_term);
            } else {
                // 从prev_log_index开始往前找，找到一个不匹配的地方
                for i in prev_log_index..=snap.snapshot_index {
                    if snap.log[(i - snap.snapshot_index) as usize].term != prev_log_term {
                        start = i + 1;
                        myprintln!("log not match and start@[{}], term is {}", start, reply.conflict_term);
                        break;
                    }
                }
            }
            reply.conflict_index = start;
        }
        self.raft.lock().unwrap().persist();

        Box::new(futures::future::result(Ok(reply)))
    }

    fn install_snapshot(&self, args: SnapshotArgs) -> RpcFuture<SnapshotReply> {
        //let mut raft = self.raft.lock().unwrap();
        let mut snap = self.raft.lock().unwrap().snapshot();
        let mut reply = SnapshotReply {
            term: snap.term,
        };
        if args.term < snap.term {
            myprintln!("warn:me[{}:{}] recv term [{}:{}]", snap.me, snap.term, args.leader_id, args.term);
            return Box::new(futures::future::result(Ok(reply)));
        }
        if args.last_included_index <= snap.snapshot_index {
            myprintln!("warn:me[{}:{}] recv snapshot [{}:{}]", snap.me, snap.snapshot_index, args.leader_id, args.last_included_index);
            return Box::new(futures::future::result(Ok(reply)));
        }
        self.msg_tx.send(false).unwrap();

        if args.term > snap.term {
            let mut raft = self.raft.lock().unwrap();
            raft.update_term_to(args.term);
            raft.vote_for(args.leader_id as i32);
            raft.persist();
            snap = raft.snapshot();
        }
        reply.term = snap.term;

        if args.last_included_index > (snap.snapshot_index + snap.log.len() as u64 - 1) {
            // replace all the log
            let log = LogEntry {
                index: args.last_included_index,
                term: args.last_included_term,
                entry: vec![],
            };
            self.raft.lock().unwrap().log = vec![log];
        }else {
            self.raft.lock().unwrap().delete_prev_log(args.last_included_index);
        }
        {
            let mut raft = self.raft.lock().unwrap();
            raft.snapshot_index = args.last_included_index;
            raft.snapshot_term = args.last_included_term;
            raft.commit_index = args.last_included_index;
            raft.last_applied = args.last_included_index;

            raft.save_state_and_snapshot(args.snapshot.clone());

            let mesg = ApplyMsg {
                command_valid: true,
                command: vec![],
                command_index: raft.snapshot_index,
                is_snapshot: true,
                snapshot: args.snapshot.clone(),
            };
            let _ret = raft.apply_ch.unbounded_send(mesg);
            myprintln!("id:{} apply snapshot:{}", raft.me, raft.snapshot_index);
        }
        Box::new(futures::future::result(Ok(reply)))
    }
}
