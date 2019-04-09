use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
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

    vote_for: i32,

    log: Vec<u64>,

    commit_index: u64,

    last_applied: u64,

    // volatile on leader
    //index of next entry to send to that server
    next_index: Option<Vec<u64>>,
    // highest log enrty replicated in that server
    match_index: Option<Vec<u64>>,

    leader_id: usize,
    // 状态
    state: Arc<State>,
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
            vote_for: -1,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
            leader_id: 0,
            state: Arc::default(),
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
        //self.persister.save_raft_state(data);
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

    fn peer_num(&self) -> u64 {
        self.peers.len() as u64
    }

    fn me(&self) -> u64 {
        self.me as u64
    }

    fn is_leader(&self) -> bool {
        self.state.is_leader()
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
    need_election: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    bg_leader_cv: Arc<(Mutex<bool>, Condvar)>,
    // Your code here.
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            need_election: Arc::new(AtomicBool::new(true)),
            shutdown: Arc::new(AtomicBool::new(false)),
            bg_leader_cv: Arc::new((Mutex::new(false), Condvar::new())),
        };
        //spawn a thread to start a election periodly

        let raft_clone = node.raft.clone();
        let need_election_1 = node.need_election.clone();
        let shutdown_1 = node.shutdown.clone();
        let bg_leader_cv_1 = node.bg_leader_cv.clone();
        thread::spawn(move || {
            loop {
                let rand_time = rand::thread_rng().gen_range(150, 300);
                let sleep_time = time::Duration::from_micros(rand_time);
                println!("sleep for {}", rand_time);
                thread::sleep(sleep_time);
                if !raft_clone.lock().unwrap().state.is_leader() && need_election_1.load(Ordering::SeqCst) {
                    let mut raft = raft_clone.lock().unwrap();
                    println!("{} start election", raft.me);
                    // launch election
                    let mut result: Vec<RequestVoteReply> = Vec::new();
                    let mut skip_count = false;
                    // ask all other server for vote
                    let peer_num = raft.peer_num();
                    println!("total peer num {}", peer_num);
                    for i in 0..peer_num {
                        if raft.me == i as usize {
                            continue;
                        }
                        println!("{} request {} for vote", raft.me, i);
                        if !need_election_1.load(Ordering::SeqCst) {
                            println!("other peer has become leader");
                            skip_count = true;
                            break;
                        }
                        let request_vote_arg = RequestVoteArgs {
                            term: raft.state.term(),
                            candidate_id: raft.me as u64,
                            last_log_index: raft.commit_index,
                            last_log_term: raft.last_applied,
                        };
                        if let Ok(reply) = raft.send_request_vote(i as usize, &request_vote_arg){
                            result.push(reply);
                            println!("{} get vote from {}", raft.me, i);
                        }
                    }
                    // this peer vote for itself
                    if !skip_count {
                        let mut count = 1;
                        for reply in result {
                            if reply.vote_granted {
                                count += 1;
                            }else{
                                println!("{} get invalid vote", raft.me);
                            }
                        }
                        if count > (peer_num + 1) / 2 {
                            // beacome leader and awake the leader thread
                            println!("{} become leader", raft.me);
                            raft.state = Arc::new(State {
                                term: raft.state.term(),
                                is_leader: true,
                            });
                            raft.leader_id = raft.me;
                            let &(ref mu, ref cv) = &*bg_leader_cv_1;
                            *mu.lock().unwrap() = true;
                            cv.notify_one();
                        }
                    }
                }
                if shutdown_1.load(Ordering::SeqCst) {
                    break;
                }
            }
        });

        let raft_clone = node.raft.clone();
        let need_election_2 = node.need_election.clone();
        let shutdown_2 = node.shutdown.clone();
        let bg_leader_cv_2 = node.bg_leader_cv.clone();
        thread::spawn(move || {
            println!("start leader thread");
            loop {
                let &(ref mu, ref cv) = &*(bg_leader_cv_2);
                let mut working = mu.lock().unwrap();
                while !*working {
                    // wait for becoming a leader
                    println!("wait for awake");
                    working = cv.wait(working).unwrap();
                }
                loop {
                    // become leader
                    let time = time::Duration::from_micros(100);
                    // refresh election flag
                    need_election_2.store(false, Ordering::SeqCst);
                    thread::sleep(time);
                    let mut raft = raft_clone.lock().unwrap();
                    println!("{} become leader", raft.me);
                    for i in 0..raft.peer_num() {
                        if i == raft.me as u64 {
                            continue;
                        };
                        println!("send heartbeat to {}", i);
                        let reply = raft.peers[i as usize]
                            .append_entries(&AppendEntriesArgs {
                                term: raft.state.term(),
                                leader_id: raft.me as u64,
                                prev_log_index: 0,
                                prev_log_term: 0,
                                entries: " ".to_string(),
                                leader_commit: 0,
                            })
                            .wait()
                            .unwrap();
                        if !reply.success {
                            panic!("heartbeat send failed");
                        }
                        if reply.term > raft.state.term() {
                            // back to follower
                            println!("{} back to follower", raft.me);
                            raft.state = Arc::new(State {
                                term: reply.term,
                                is_leader: false,
                            });
                            *working = false;
                            break;
                        }
                    }
                    if !raft.state.is_leader() {
                        break;
                    }
                    if shutdown_2.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
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
        self.raft.lock().unwrap().leader_id == self.raft.lock().unwrap().me
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
        self.raft.lock().unwrap().persist();
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

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
impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        println!("get vote req from {}", args.candidate_id);
        let mut lock = self.raft.try_lock();
        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };
        if let Ok(ref mut raft) = lock{
            let term = raft.state.term();
            let vote_for = raft.vote_for;
            if (args.term >= term) && (vote_for == -1) {
                reply.term = term;
                reply.vote_granted = true;
                raft.vote_for = args.candidate_id as i32;
            }
            println!("reply vote req for {}", args.candidate_id);
            Box::new(futures::future::result(Ok(reply)))
            // 收到别人的request vote调用
            // 如果当前term还没投票则投票，否则不投票
        }else{
            Box::new(futures::future::result(Ok(reply)))
        }
        
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        println!("get heartbeat from {}", args.leader_id);
        let mut raft = self.raft.lock().unwrap();
        let term = raft.state.term();
        let mut reply = AppendEntriesReply {
            term: term,
            success: false,
        };
        if args.term >= term {
            // refresh election flag as false
            self.need_election.store(false, Ordering::SeqCst);
            raft.leader_id = args.leader_id as usize;
            if term != args.term {
                raft.state = Arc::new(State {
                    term: args.term,
                    is_leader: false,
                });
            }
            reply.success = true;
        }
        println!("reply for {}", args.leader_id);
        Box::new(futures::future::result(Ok(reply)))
    }
}
