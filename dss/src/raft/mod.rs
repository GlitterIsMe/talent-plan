use std::sync::{
    Arc,
    Atomic::{AtomicBool, Ordering},
    Condvar,
};
use std::{thread, time};

use random::Source;

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
    pub vote_for: i32,
    pub log: Vec<u64>,
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

    pub fn vote_for(&self) -> i32 {
        self.vote_for
    }
}

pub struct VolatileState {
    // volatile in all server
    pub commit_index: u64,
    pub last_applied: u64,

    // volatile on leader
    pub next_index: Option<Vec<u64>>, //index of next entry to send to that server
    pub match_index: Option<Vec<u64>>, // highest log enrty replicated in that server
}

impl VolatileState {
    fn new() -> VolatileState {
        VolatileState {
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
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
    // 状态
    state: Arc<State>,

    vstate: Arc<VolatileState>,

    election_flag: Arc<Mutex<bool>>,

    shutdown: Arc<AtomicBool>,

    leader_thread: Arc<(Mutex(bool), Condvar)>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
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
            state: Arc::default(),
            vstate: Arc::new(VolatileState::new()),
            election_flag: Arc::new(Mutex::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            leader_thread: Arc::new((Mutex::new(false), Condvar::new())),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        let need_election = rf.election_flag.clone();
        let peer_num = rf.peer.len();
        let this_peer = rf.me;

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    /// 存储持久化的信息
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    /// 重新获取之前保存的持久化信息
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
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
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             OK(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    // append_entry
    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn work(&mut self) {
        // get a random duration
        // launch a election if there is no leade after this duration
        loop {
            let sleep_time =
                time::Duration::from_micros(random::default().seed([150, 300]).read::<u64>());
            thread::sleep(sleep_time);
            if !self.is_leader() && self.need_election() {
                // launch election
                let result: Vec<RequestVoteReply> = Vec::new();
                // ask all other server for vote
                for i in 0..self.peers.len() {
                    if i == self.me {
                        continue;
                    }
                    let request_vote_arg = RequestVoteArgs {
                        term: self.term(),
                        condidateId: self.me,
                        lastLogIndex: self.vstate.commit_index,
                        lastLogTerm: self.vstate.last_applied,
                    };
                    result.push_back(self.send_request_vote(i, &request_vote_arg));
                }
                let mut count = 0;
                for reply in result {
                    if reply.voteGranted {
                        count += 1;
                    }
                }
                if count > (self.peer_num() + 1) / 2 {
                    // beacome leader and awake the leader thread
                    self.state.is_leader = true;
                    let &(ref mu, ref cv) = &*self.leader_thread;
                    mu.lock().unwrap() = true;
                    cv.notify_one();
                }
            }
            if self.shutdown.load(Ordering::SeqCst) {
                break;
            }
        }
    }

    fn leader_work(&mut self) {
        loop {
            let &(ref mu, ref cv) = &*(self.leader_work);
            let mut working = mu.lock().unwrap();
            while !*working {
                // wait for becoming a leader
                working = cv.wait(working).unwrap();
            }
            loop {
                // become leader
                let time = time::Duration::from_micros(100);
                self.election_flag = false;
                thread::sleep(time);
                for i in i..self.peer_num() {
                    if i == me {
                        continue;
                    };
                    let reply = self.peer[i].append_entries(AppendEntriesArg {
                        term: self.state.term(),
                        leader_id: self.me,
                        prevLogIndex: 0,
                        prevLogTerm: 0,
                        entries: None,
                        leaderCommit: 0,
                    });
                    if !reply.success {
                        panic!("heartbeat send failed");
                    }
                    if reply.term > self.state.term() {
                        // back to follower
                        self.state.is_leader = false;
                        *working.lock().unwrap() = false;
                        break;
                    }
                }
                if !self.state.is_leader() {
                    break;
                }
            }
            if self.shutdown.load(Ordering::SeqCst) {
                break;
            }
        }
    }

    fn need_election(&self) -> bool {
        return self.election_flag.lock().unwrap();
    }

    fn peer_num(&self) -> u64 {
        return self.peers.len();
    }

    fn me(&self) -> u64 {
        return self.me;
    }

    fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
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
    raft: Arc<Mutex<Raft>>,
    is_leader: bool,
    // Your code here.
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(Mutex::new()),
        };
        //spawn a thread to start a election periodly

        let raft_clone = node.raft.clone();
        thread::spwan(move || {
            raft_clone.work();
        });

        thread::spawn(move || {
            raft_clone.leader_work();
        });

        node
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
        self.raft.start(command);
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // unimplemented!()
        self.raft.state.term();
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        self.raft.leader_id == self.raft.me;
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
        raft.shutdown();
    }
}

// 为什么impl for Node？难道不是Raft
/* impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        unimplemented!()
        // 收到别人的request vote调用
        // 如果当前term还没投票则投票，否则不投票
    }

    fn append_entries(&self, args: AppendEntriesArg) -> RpcFuture<AppendEntriesApply>{

    }
} */
impl RaftService for Raft {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut reply = RequestVoteReply {
            term: self.state.term(),
            voteGranted: false,
        };
        if args.term >= self.state.term() {
            if self.state.vote_for() == -1 {
                reply.term = self.state.term();
                reply.voteGranted = true;
                self.state.vote_for = args.candidateId;
            }
        }
        Box::new(futures::future::result(Ok(reply)))
        // 收到别人的request vote调用
        // 如果当前term还没投票则投票，否则不投票
    }

    fn append_entries(&self, args: AppendEntriesArg) -> RpcFuture<AppendEntriesApply> {
        let mut reply = RequestVoteReply {
            term: self.state.term(),
            success: false,
        };
        if args.term >= self.state.term() {
            // refresh election flag as false
            *election_flag.lock().unwrap() = false;
            if self.state.term() != args.term {
                self.state.term = args.term;
            }
            reply.success = true;
        }
        Box::new(futures::future::result(Ok(reply)))
    }
}
