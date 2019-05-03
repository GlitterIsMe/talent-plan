use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::{thread, time};
use std::time::Duration;

use rand::{Rng, thread_rng};

use futures::sync::{mpsc::{unbounded, UnboundedSender, UnboundedReceiver}, oneshot};
use futures::Future;
use futures::future::*;
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
use futures_timer::Delay;
use futures_timer::ext::Timeout;


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

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(bytes, tag = "2")]
    pub entry: Vec<u8>,
}

impl LogEntry {
    fn new() -> Self {
        LogEntry {
            term: 0,
            entry: vec![],
        }
    }
}

#[derive(Debug)]
pub enum RaftMessage {
    AppendEntry(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    RequestVote(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
    AppendReply(AppendEntriesReply, usize),
    VoteReply(RequestVoteReply),
    Start(LogEntry),
}

#[derive(Debug)]
pub enum Command {
    Msg(RaftMessage),
    Shutdown,
}

#[derive(Debug)]
pub enum Task {
    Msg(RaftMessage),
    ElectionTimeout,
    HeartbeatTimeout,
}

/// Persistent State of a raft peer.
#[derive(Clone, Message)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(int32, tag = "2")]
    pub vote_for: i32,

    #[prost(bytes, repeated, tag = "3")]
    pub log: Vec<LogEntry>,
}

/// State of a raft peer.
#[derive(Clone, Default, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,

}

impl State {
    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Clone, Debug)]
pub struct ShareState {
    pub state: State,
    pub last_index: u64,
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum Role {
    FOLLOWER = 1,
    CANDIDATE = 2,
    LEADER = 3,
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

    state: PersistentState,

    last_index: u64,

    commit_index: u64,

    last_applied: u64,

    // volatile on leader
    //index of next entry to send to that server
    next_index: Vec<u64>,
    // highest log enrty replicated in that server
    match_index: Vec<u64>,

    leader_id: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,

    vote_count: u64,
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
        let mut rf = Raft {
            peers,
            persister,
            me,
            apply_ch,
            state: PersistentState {
                term: 0,
                vote_for: -1,
                log: vec![LogEntry::new()],
            },
            last_index: 0,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; peers.len()],
            match_index: vec![0; peers.len()],
            leader_id: 0,
            role: Role::FOLLOWER,
            vote_count: 0,
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
        myprintln!("persist {}", self.me);
        labcodec::encode(&self.state, &mut raw_data).unwrap();
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

        let state: PersistentState = labcodec::decode(data).unwrap();
        for log in &state.log {
            self.last_index += 1;
        }

        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
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
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs, sender: &UnboundedSender<Command>) {
        let peer = &self.peers[server];
        let future = peer
            .request_vote(&args)
            .and_then(|reply| {
                sender.unbounded_send(Command::Msg(RaftMessage::VoteReply(reply)));
                Ok(());
            })
            .map_err(|err| {
                println!("request vote error : {:?}", err);
            });
        peer.spawn(future);
    }

    fn send_append_entries(
        &mut self,
        server: usize,
        args: &AppendEntriesArgs,
        sender: &UnboundedSender<Command>,
    ) {
        let peer = &self.peers[server];
        let future = peer
            .append_entries(&args)
            .and_then(|reply| {
                if reply.success {
                    // majority of server has reply success
                    myprintln!("receive success from {}", server);
                    if args.entries.len() > 0 {
                        let next = self.raft.next_index(server as usize);
                        self.set_next_index(server as usize, next + 1);
                        self.set_match_index(server as usize, next);
                        self.update_commit_index();
                        myprintln!("{} update next index to {}, match index to {}", server, next + 1, next);
                    }
                } else {
                    // append false
                    if self.role != Role::LEADER {
                        myprintln!("{} not a leader any more and quit directly", self.me);
                        return;
                    }
                    if reply.term > self.state.term {
                        myprintln!("{} become follower because term", self.me);
                        self.update_term_to(reply.term, self.clone());
                        self.transfer_state(Role::FOLLOWER, self.clone());
                    } else {
                        let next = self.next_index(server as usize);
                        if next != 0 {
                            self.set_next_index(server as usize, next - 1);
                            //raft.set_match_index(i as usize, next);
                            myprintln!("{} rollback next index to {}", server, next -1);
                        }
                    }
                }
                //sender.unbounded_send(Command::Msg(RaftMessage::AppendReply(reply, server)));
                Ok(());
            })
            .map_err(|err| {
                println!("send append entris error : {:?}", err);
            });
        peer.spawn(future);
    }

    // append_entry
    /*fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
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
    }*/

    fn term(&self) -> u64 {
        self.state.term
    }

    fn update_term_to(&mut self, new_term: u64, state: Arc<Mutex<ShareState>>) {
        self.state.term = new_term;
        // 刚更新term之后一定没有投票
        self.vote_count = 0;
        self.state.vote_for = -1;
        state.lock().unwrap().state.term = new_term;
    }

    fn init_tow_index(&mut self) {
        for i in 0..self.peers.len() {
            self.next_index[i] = self.next_index(i) + 1;
            self.match_index[i] = 0;
        }
    }

    fn transfer_state(&mut self, new_role: Role, state: Arc<Mutex<ShareState>>) {
        match new_role {
            Role::FOLLOWER => {
                myprintln!("{} transfer to follower, term[{}]", self.me, self.state.term);
                self.role = Role::FOLLOWER;
                state.lock().unwrap().state.is_leader = false;
            }
            Role::CANDIDATE => {
                myprintln!("{} transfer to candidate", self.me);
                self.role = Role::CANDIDATE;
                state.lock().unwrap().state.is_leader = false;
            }
            Role::LEADER => {
                myprintln!("{} transfer to leader", self.me);
                self.role = Role::LEADER;
                self.leader_id = self.me as usize;
                state.lock().unwrap().state.is_leader = true;
                self.init_tow_index();
            }
        }
        //self.persist();
    }

    fn update_commit_index(&mut self) {
        let last_index = self.last_index;
        let commit_index = self.commit_index;
        for i in ((commit_index + 1)..=last_index).rev() {
            myprintln!("check for index {}", i);
            let mut matched = 1;
            let index = &mut self.match_index;
            for match_i in index {
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
            self.apply_ch.unbounded_send(msg)
                .map(move |_| println!("apply a log index"))
                .map_err(|e| println!("error = {:?}", e))
                .unwrap();
//            self.persist();
        }
    }

    fn me(&self) -> u64 {
        self.me as u64
    }

    fn is_leader(&self) -> bool {
        self.leader_id == self.me
    }

    fn last_log_index(&self) -> u64 {
        self.last_index
    }

    fn last_log_term(&self) -> u64 {
        self.log[self.last_index as usize].term
    }

    fn get_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.state.term,
            candidate_id: self.me as u64,
            last_log_index: self.last_index,
            last_log_term: self.log[self.last_index as usize].term,
        }
    }

    fn get_append_args(&self, i: u64) -> AppendEntriesArgs {
        let mut args = AppendEntriesArgs {
            term: self.state.term(),
            leader_id: self.me as u64,
            prev_log_index: self.next_index(i as usize) - 1, // prev log index is the prev index of next_index[i]
            prev_log_term: self.log[(self.next_index(i as usize) - 1) as usize].term, // prev log index is the prev index of next_index[i],
            entries: vec![],
            entry_term: 0,
            leader_commit: self.commit_index as u64, // index of leader has commited
        };
        let index = self.next_index[i as usize];
        myprintln!("{} next index is {}", i, index);
        if self.next_index(i as usize) == self.last_index + 1 {
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

enum TimeoutType {
    Election,
    Heartbeat,
}

const MAX_ELECTION_TIMEOUT: u64 = 300;
const MIN_ELECTION_TIMEOUT: u64 = 150;
const HEARTBEAT_TIMEOUT: u64 = 50;

struct Store {
    raft: Raft,
    sender: UnboundedSender<Command>,
    state: Arc<Mutex<ShareState>>,
}

impl Store {
    fn new(raft: Raft, sender: UnboundedSender<Command>, state: Arc<Mutex<ShareState>>) -> Self {
        {
            let state = state.lock().unwrap();
            state.state.term = raft.state.term;
            state.state.is_leader = false;
            state.last_index = raft.last_index;
        }
        Store {
            raft,
            sender,
            state,
        }
    }

    fn get_random_time(timeout: TimeoutType) -> Duration {
        let mut time;
        let time = match timeout {
            TimeoutType::ElectionTimeout => {
                let mut rng = thread_rng();
                rng.gen_range(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
            }
            TimeoutType::HeartbeatTimeout => {
                HEARTBEAT_TIMEOUT
            }
        };
        Duration::from_millis(time)
    }

    pub fn new_timeout(&self) -> (Delay, TimeoutType) {
        match self.raft.role {
            Role::LEADER => (Delay::new(Store::get_random_time(TimeoutType::Heartbeat)), TimeoutType::Heartbeat),
            Role::CANDIDATE
            | Role::FOLLOWER => (Delay::new(Store::get_random_time(TimeoutType::Election)), TimeoutType::Election),
        }
    }

    pub fn process_task(&mut self, task: Task) {
        match task {
            Task::Msg(msg) => {
                match msg {
                    RaftMessage::AppendEntry(args, sender) => {
                        self.reply_for_append_entry(&args, sender);
                    }
                    RaftMessage::RequestVote(args, sender) => {
                        // start election
                        self.reply_for_vote_request(&args, sender);
                    }
                    RaftMessage::AppendReply(reply, server) => {
                        // handle a append reply
                        //self.process_append_reply(reply, server);
                        panic!("no this possible");
                    }
                    RaftMessage::VoteReply(reply) => {
                        // handle a vote reoky
                        self.process_vote_reply(reply);
                    }
                    RaftMessage::Start(log) => {
                        // start a log
                        self.statr_entry(log);
                    }
                }
            }
            Task::ElectionTimeout => {
                match self.raft.role {
                    Role::FOLLOWER => {
                        // start an election
                        self.raft.transfer_state(Role::CANDIDATE, self.state.clone());
                        self.broadcast_vote();
                    }
                    Role::CANDIDATE => {
                        // start an election
                        self.broadcast_vote();
                    }
                    _ => {
                        panic!("Leader get an ElectionTimeout");
                    }
                }
            }
            Task::HeartbeatTimeout => {
                match self.raft.role {
                    Role::FOLLOWER
                    | Role::CANDIDATE => {
                        panic!("Follower or Candidate get a HeartbeatTimeout");
                    }
                    Role::LEADER => {
                        // start an append entry
                        self.broadcast_append_entries()
                    }
                }
            }
        }
        self.persist();
    }

    fn broadcast_prevote(&mut self, args: RequestVoteArgs) {
        unimplemented!()
    }

    fn broadcast_vote(&mut self) {
        let mut vote_arg = self.raft.get_vote_args();
        self.raft.update_term_to(self.raft.term() + 1, self.state.clone());
        // 首先为自己投票
        self.raft.state.vote_for = self.raft.me as i32;
        self.raft.vote_count += 1;
        myprintln!("{} start election", self.raft.me);
        let vote_count = Arc::new(AtomicUsize::new(1));
        for i in 0..self.raft.peers.len() {
            if i == self.raft.me as usize {
                continue;
            }
            if self.raft.role != Role::CANDIDATE {
                return;
            }
            let args = vote_arg.clone();
            // 需要把self.sender传过去，不然没法发消息到receiver
            self.raft.send_request_vote(i, &args, &self.sender);
        }
    }

    fn broadcast_append_entries(&mut self) {
        myprintln!("{} start append entry call, term[{}]", self.raft.me, self.raft.state.term);
        for i in 0..self.raft.peers.len() {
            if i == self.raft.me {
                continue;
            }
            // if this peer is not leader anymore stop this append
            if self.raft.role != Role::LEADER {
                return;
            }
            let mut append_args = self.raft.get_append_args(i as u64);
            self.raft.send_append_entries(i, &append_args, &self.sender);
        }
        myprintln!("{} end this heartbeat", self.raft.me);
    }

    fn reply_for_vote_request(&mut self, args: &RequestVoteArgs, sender: oneshot::Sender<RequestVoteReply>) {
        let mut reply = RequestVoteReply {
            term: self.raft.state.term,
            vote_granted: false,
        };
        myprintln!(
                "{}[term {}] get vote req from {}[term {}]",
                self.raft.me,
                self.raft.state.term,
                args.candidate_id,
                args.term
            );
        if self.raft.state.term > args.term {
            // term is larger than args
            myprintln!("{} reply {} because larger term {} > {}", self.raft.me, reply.vote_granted, self.state.term, args.term);
            sender.send(reply);
            return;
        } else if self.raft.state.term < args.term {
            // term is less smaller than args
            myprintln!("{} update term to {} and transfer to follower", self.raft.me, args.term);
            self.raft.update_term_to(args.term, self.state.clone());
            self.raft.transfer_state(Role::FOLLOWER, self.state.clone());
        }

        let mut up_to_date = true;

        if self.raft.last_log_term() > args.last_log_term {
            up_to_date = false;
            myprintln!("{} reject vote because larger last log term {} > {}",
                          self.raft.me,
                          self.raft.last_log_term(),
                          args.last_log_term);
        } else if self.raft.last_log_term() == args.last_log_term {
            if self.raft.last_log_index() > args.last_log_index {
                up_to_date = false;
                myprintln!("{} reject vote because larger last log index {} > {}",
                                self.raft.me,
                                self.raft.last_log_index(),
                                args.last_log_index);
            }
        }

        if (self.raft.vote_for == -1 || self.raft.vote_for == args.candidate_id as i32) && up_to_date {
            myprintln!("{} granted vote for {}", self.raft.me, args.candidate_id);
            reply.vote_granted = true;
        }

        myprintln!("{} reply {}", self.raft.me, reply.vote_granted);
        sender.send(reply);
    }

    fn reply_for_append_entry(&mut self, args: &AppendEntriesArgs, sender: oneshot::Sender<AppendEntriesReply>) {
        if args.entries.len() == 0 {
            myprintln!("{} get heartbeat from {}", self.raft.me, args.leader_id);
        } else {
            myprintln!("{} get log from {}", self.raft.me, args.leader_id);
        }

        let last_log_index = self.raft.last_index;
        let last_log_term = self.raft.log[last_log_index as usize].term;
        myprintln!("{} got last_index[{}] and last term [{}]", self.raft.me, last_log_index, last_log_term);
        if self.raft.leader_id != args.leader_id as usize {
            // update leader id
            self.raft.leader_id = args.leader_id as usize
        };

        let mut reply = AppendEntriesReply {
            term: self.raft.state.term,
            success: false,
        };
        if args.term < reply.term {
            // false
            myprintln!("{} reply false for {} because term {} > {}", self.raft.me, args.leader_id, reply.term, args.term);
            sender.send(reply);
            return;
        } else if args.term > reply.term {
            self.raft.update_term_to(args.term, self.state.clone());
            if self.raft.role != Role::FOLLOWER {
                self.raft.transfer_state(Role::FOLLOWER, self.state.clone());
            }
        } else {
            reply.success = true;
        }
        // do log consistency check
        if last_log_index < args.prev_log_index {
            //log不够，返回并next_index -1重来
            myprintln!(
                    "{} reply false for {} because log lost:[index:{}-{}]",
                    self.raft.me,
                    args.leader_id,
                    last_log_index,
                    args.prev_log_index,
                );
            reply.success = false;
            sender.send(reply);
            return;
        }
        // 一定是last_log_index大于等于prev_log_index的情况
        let prev_log_term;
        myprintln!("get prev log term of {} at {} and log len is {}", self.raft.me, args.prev_log_index, self.raft.log.len());
        prev_log_term = self.raft.log[args.prev_log_index as usize].term;
        if prev_log_term != args.prev_log_term {
            // prev_log_term不匹配，回退重试
            myprintln!(
                    "{} reply false for {} because unmatched term:[term: {}-{}]",
                    self.raft.me,
                    args.leader_id,
                    prev_log_term,
                    args.prev_log_term
                );
            reply.success = false;
            sender.send(reply);
            return;
        }
        //do log replicate
        myprintln!(
                "{} append log, last index[{}], log len[{}], commit_index[{}]",
                self.raft.me,
                self.raft.last_index,
                self.raft.log.len(),
                self.raft.commit_index
            );
        if self.raft.log.len() > (args.prev_log_index + 1) as usize {
            // delete wrong log
            let mut del_count = 0;
            for i in args.prev_log_index + 1..(self.raft.log.len() as u64) {
                myprintln!("{} remove log in {}", self.raft.me, i);
                self.raft.log.remove(args.prev_log_index as usize);
                del_count += 1;
            }
            self.raft.last_index -= del_count;
        }
        let pos = args.prev_log_index + 1;
        if args.entries.len() != 0 {
            self.raft.log.reserve(1);
            self.raft.log.append(&mut vec![LogEntry::from_data(args.entry_term, pos + 1, &args.entries)]);
            self.raft.last_index = args.prev_log_index + 1;
            myprintln!("{} last index update to prev_log_index[{}] + 1", self.raft.me, args.prev_log_index);
        }
        reply.success = true;

        if args.leader_commit > self.raft.commit_index as u64 {
            self.raft.commit_index = std::cmp::min(args.leader_commit, args.prev_log_index + 1);
            myprintln!("update commit index to {}", self.raft.commit_index);
        }
        reply.success = true;
        myprintln!("reply heartbeat success");
        sender.send(reply);
    }

    fn process_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term > self.raft.state.term {
            myprintln!("get a larger term {} form vote reply and update from {}", reply.term, self.raft.state.term);
            self.raft.update_term_to(reply.term, self.state.clone());
            self.raft.transfer_state(Role::FOLLOWER, self.state.clone());
        } else if reply.vote_granted {
            if self.raft.role == Role::LEADER {
                return;
            }
            self.raft.vote_count += 1;
            myprintln!("{} got {} votes", self.raft.me, self.raft.vote_count);
            if self.raft.vote_count > self.raft.peers.len() / 2 {
                myprintln!("receive majority vote {} and become leader", self.raft.vote_count);
                self.raft.transfer_state(Role::LEADER, self.state.clone());
            }
        }
    }

    fn process_append_reply(&mut self, reply: AppendEntriesReply, server: usize) {
        /*if reply.success {
            // majority of server has reply success
            myprintln!("receive success from {}", server);
            if append_args.entries.len() > 0 {
                let next = self.raft.next_index(server as usize);
                self.raft.set_next_index(server as usize, next + 1);
                self.raft.set_match_index(server as usize, next);
                self.raft.update_commit_index();
                myprintln!("{} update next index to {}, match index to {}", server, next + 1, next);
            }
        } else {
            // append false
            if self.raft.role != Role::LEADER {
                myprintln!("{} not a leader any more and quit directly", self.raft.me);
                return;
            }
            if reply.term > self.raft.state.term {
                myprintln!("{} become follower because term", self.raft.me);
                self.raft.update_term_to(reply.term, self.state.clone());
                self.raft.transfer_state(Role::FOLLOWER, self.state.clone());
            } else {
                let next = self.raft.next_index(server as usize);
                if next != 0 {
                    self.raft.set_next_index(server as usize, next - 1);
                    //raft.set_match_index(i as usize, next);
                    myprintln!("{} rollback next index to {}", server, next -1);
                }
            }
        }*/
    }

    fn statr_entry(&mut self, entry: LogEntry) {
        self.raft.log.reserve(2);
        self.raft.log.insert((self.raft.last_index + 1) as usize, entry);
        self.raft.last_index += 1;
        self.state.lock().unwrap().last_index = self.raft.last_index;
    }

    fn persist(&mut self) {
        self.raft.persist();
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

#[derive(Clone)]
pub struct Node {
    //raft: Arc<Mutex<Raft>>,
    //timeout_tx: Sender<u32>,
    //shutdown: Arc<AtomicBool>,
    // Your code here.
    sender: UnboundedSender<Command>,
    state: Arc<Mutex<ShareState>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (sender, receiver) = unbounded::<Command>();
        let state = Arc::new(Mutex::new(ShareState {
            state: State {
                term: 0,
                is_leader: false,
            },
            last_index: 0,
        }));
        let store = Store::new(raft, sender, state.clone());
        let sender = sender.clone();
        let node = Node {
            sender,
            state,
        };
        //spawn a thread to start a election periodly
        thread::spawn(move || {
            Node::raft_main(raft, state.clone(), sender, receiver);
        });
        node
    }

    fn raft_main(raft: Raft, state: Arc<Mutex<ShareState>>, sender: UnboundedSender<Command>, receiver: UnboundedReceiver<Command>) {
        //let (sender, receiver) = unbounded::<Command>();
        let mut store = Store::new(raft, sender, state);
        let mut receiver = receiver.into_future();
        let mut shutdown = false;
        loop {
            let (timeout, t) = store.new_timeout();
            let (task, future_receiver) = receiver.select2(timeout).wait().and_then(|res| {
                match res {
                    Ok(Either::A((Command::Msg(msg), future_stream))/*return by receiver*/, _/*return by timeout*/) => {
                        (Task::Msg(msg), future_stream)
                    }
                    Ok(Either::A((Command::Shutdown, future_stream), _)) => {
                        shutdown = true;
                        ((), future_stream)
                    }
                    Ok(Either::B((_, future_stream))) => {
                        match t {
                            TimeoutType::Election => (Task::HeartbeatTimeout, future_stream),
                            TimeoutType::Heartbeat => (Task::ElectionTimeout, future_stream),
                        }
                    }
                    Err(Either::A((err, stream), _)) => {
                        panic!("receive error {:?}", err);
                    }
                    Err(Either::B((_, stream))) => {
                        panic!("timeout error {:?}", err);
                    }
                }
            });
            if shutdown {
                return;
            }
            receiver = future_receiver;
            store.process_task(task);
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
        if self.state.lock().unwrap().state.is_leader {
            myprintln!("start an entry");
            // only leader can serve client
            // calculate index
            let index: u64;
            let term: u64;
            {
                index = self.state.lock().unwrap().last_index + 1;
                term = self.state.lock().unwrap().state.term;
            }

            let mut buf = vec![];
            myprintln!("got command {:?}", command);
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // cache this log to log[]
            // construct a log entry
            let log_entry =
                LogEntry::from_data(self.state.lock().unwrap().state.term, (self.state.lock().unwrap().last_index + 1) as u64, &buf);
            // insert to log[]
            /*self.log.reserve(2);
            self.log.insert((self.last_index + 1) as usize, log_entry);
            self.last_index += 1;*/
            //myprintln!("{} log vec len is {}", self.me(), self.log.len());
            //myprintln!("{} last index is {}", self.me(), self.last_index);
            // return Ok
            self.sender.unbounded_send(Command::Msg(RaftMessage::Start(log_entry)));
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // unimplemented!()
        self.state.lock().unwrap().state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        self.state.lock().unwrap().state.is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let state = self.state.lock().unwrap();
        State {
            term: state.state.term,
            is_leader: state.state.is_leader,
        }
    }

    /// the tester calls kill() when a Raft instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.sender.unbounded_send(Command::Shutdown);
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let (sender, receiver) = oneshot::channel();
        self.sender.unbounded_send(Command::Msg(RaftMessage::RequestVote(args, sender)));
        receiver
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let (sender, receiver) = oneshot::channel();
        self.sender.unbounded_send(Command::Msg(RaftMessage::AppendEntry(args, sender)));
        receiver
    }
}
