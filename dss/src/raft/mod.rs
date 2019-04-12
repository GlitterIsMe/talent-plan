use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
    mpsc::{channel, Sender}
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
    ($($arg: tt)*) => (
        println!("Debug({}:{}): {}", file!(), line!(), format_args!($($arg)*));
    ) 
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

#[derive(Clone)]
pub struct LogEntry{
    term: u64,
    index: u64,
    entry: Vec<u8>,
}

impl LogEntry{
    fn new() -> Self{
        LogEntry{
            term: 0,
            index: 0,
            entry: vec![],
        }
    }

    fn from_data(term: u64, index: u64, src_entry: &Vec<u8>) -> Self{
        LogEntry{
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

    commit_index: Arc<AtomicUsize>,

    last_applied: u64,

    // volatile on leader
    //index of next entry to send to that server
    next_index: Option<Vec<Arc<Mutex<u64>>>>,
    // highest log enrty replicated in that server
    match_index: Option<Vec<Arc<Mutex<u64>>>>,

    leader_id: usize,
    // 状态
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,
    next_role: Arc<Mutex<Role>>,
    vote_count: Arc<AtomicUsize>,
    vote_req: Arc<AtomicUsize>,
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
            vote_for: -1,
            log: vec![LogEntry::new()],
            last_index: 0,
            commit_index: Arc::new(AtomicUsize::new(0)),
            last_applied: 0,
            next_index: None,
            match_index: None,
            leader_id: 0,
            state: Arc::default(),
            role: Role::FOLLOWER,
            next_role: Arc::new(Mutex::new(Role::FOLLOWER)),
            vote_count: Arc::new(AtomicUsize::new(1)),
            vote_req: Arc::new(AtomicUsize::new(0)),
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
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    fn send_append_entries(&self, server: usize, args: &AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let peer = &self.peers[server];
        peer.append_entries(&args).map_err(Error::Rpc).wait()
    }

    // append_entry
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.state.is_leader(){
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
            let log_entry = LogEntry::from_data(self.state.term(), (self.log.len() + 1) as u64, &buf);
            // insert to log[]
            self.log.reserve(2);
            self.log.insert((self.last_index + 1) as usize, log_entry);
            self.last_index += 1;
            myprintln!("{} log vec len is {}",self.me(), self.log.len());
            myprintln!("{} last index is {}",self.me(), self.last_index);
            // return Ok
            Ok((index, term))
        }else{
            Err(Error::NotLeader)
        }
    }

    fn transfer_state(&mut self, new_role: Role){
        let old_role = self.role;
        match new_role{
            Role::FOLLOWER => {
                
                {
                    myprintln!("{} transfer to follower", self.me);
                    self.vote_count.store(1, Ordering::SeqCst);
                    self.role = Role::FOLLOWER;
                    *self.next_role.lock().unwrap() = self.role;
                }
            },
            Role::CANDIDATE => {
                {
                    // TODO: CANDIDATE also has a timeout
                    myprintln!("{} transfer to candidate", self.me);
                    self.role = Role::CANDIDATE;
                    *self.next_role.lock().unwrap() = self.role;
                    self.start_election();
                }
            },
            Role::LEADER => {
                {
                    // become leader
                    myprintln!("{} transfer to leader", self.me);
                    self.role = Role::LEADER;
                    *self.next_role.lock().unwrap() = self.role;
                    {
                        // update state
                        self.state = Arc::new(State{
                            term: self.state.term(),
                            is_leader: true,
                        });
                        self.leader_id = self.me;
                        self.vote_for = self.me as i32;
                        // self.commit_index = 0; commit index won't change because of becoming leader
                        self.last_applied = 0;
                        // init next index to send to each server
                        myprintln!("init next index to {}", self.last_index + 1);
                        self.next_index = Some(vec![]);
                        self.match_index = Some(vec![]);
                        for i in 0..self.peers.len(){
                            if let Some(x) = &mut self.next_index{
                                x.push(Arc::new(Mutex::new(self.last_index + 1)));
                            }
                            if let Some(y) = &mut self.match_index{
                                y.push(Arc::new(Mutex::new(0)));
                            }
                        }
                    }
                }
            },
        }
    }

    fn start_election(&mut self){
        // call request_vote for every sever
        myprintln!("{} start election", self.me);
        let mut vote_arg = RequestVoteArgs{
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        let peer_num;
        let me;
        {
            peer_num = self.peers.len();
            me = self.me; 
            // increment term
            self.state = Arc::new(State{
                term: self.state.term() + 1,
                is_leader: false,
            });
            vote_arg.term = self.state.term();
            vote_arg.candidate_id = me as u64;
            vote_arg.last_log_term = if self.last_index == 0 || self.last_index == 1{
                                            self.state.term()
                                    }else{
                                        self.log[self.last_index as usize].term
                                    };
            vote_arg.last_log_index = self.last_index;
        }
        for i in 0..peer_num{
            if i == me as usize {continue;}
            let vote_count = self.vote_count.clone();
            let vote_req = self.vote_req.clone();
            let peer = self.peers[i].clone();
            let args = vote_arg.clone();
            thread::spawn(move || {
                //线程里面不能有self
                myprintln!("request {} for vote", i);
                match peer.request_vote(&args).map_err(Error::Rpc).wait(){
                    Ok(reply) =>{
                        if reply.vote_granted{
                            //myprintln!("{} for {}", i, me);
                            vote_count.store(vote_count.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
                            myprintln!("{} got {} votes", me, vote_count.load(Ordering::SeqCst));
                        }
                    },
                    Err(e) =>{
                        myprintln!("failed to get vote result because {:?}", e);
                    },
                }
                vote_req.store(vote_count.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
            });
        }
        // each call is a thread
        // thread communicate by channel
    }

    fn start_append_entry(&mut self/*, finish_append: Sender<u32>*/){
        myprintln!("{} start append entry call, term[{}]", self.me, self.state.term());
        let reply_server = Arc::new(AtomicUsize::new(0));
        for i in 0..self.peers.len(){
            if i == self.me as usize {continue;}
            {
                // if this peer is not leader anymore stop this append
                if self.role != Role::LEADER {return;}
            }
            // cunstruct an arg
            myprintln!("last index is {}", self.last_index);
            let mut send_entry_index = 0;
            let append_args = AppendEntriesArgs{
                term: self.state.term(),// term is the now term of leader
                leader_id: self.me as u64,
                prev_log_index: self.last_index,// prev log index is the prev of newest one entry
                prev_log_term: if self.last_index == 0 || self.last_index == 1 {
                                    self.state.term()// if there is no entry then term is the term of now
                                }else{
                                    myprintln!("got prev log[{}] term {}", 
                                            self.last_index - 1, 
                                            self.log[(self.last_index - 1) as usize].term);
                                    self.log[(self.last_index - 1) as usize].term // or term is the term of last entry
                                },
                entries: if let Some(index) = &self.next_index{
                            //let index = *(index[i].lock().unwrap());
                            send_entry_index = *(index[i].lock().unwrap()); 
                            myprintln!("{} next index is {}", i, send_entry_index);
                            if send_entry_index == self.last_index + 1{
                                // next index of this server is the newest, then send heartbeat 
                                myprintln!("{} start heartbeat", self.me);
                                vec![]
                            }else{
                                 // or send the corresponding entry
                                myprintln!("{} start append entry", self.me);
                                self.log[send_entry_index as usize].entry.clone()
                            }
                        }else{
                            myprintln!("{} start hearbeat", self.me);
                           vec![]
                        },
                leader_commit: self.commit_index.load(Ordering::SeqCst) as u64,// index of leader has commited
            };
   
            let peer = self.peers[i].clone();
            //let tx = tx.clone();
            let next_inedx_i = if let Some(index) = &self.next_index{
                index[i].clone()
            } else{
                panic!("Access next index but not inited");
            };
            let match_index_i = if let Some(index) = &self.match_index{
                index[i].clone()
            }else{
                panic!("Access match index but not inited");
            };
            let peer_num = self.peer_num();
            let reply_server = reply_server.clone();
            let commit_index = self.commit_index.clone();
            let last_index = self.last_index;
            let next_role = self.next_role.clone();
            let term = self.state.term();
            let me = self.me;
            thread::spawn(move || {
                match peer.append_entries(&append_args).map_err(Error::Rpc).wait(){
                    Ok(reply) => {
                        if reply.success && append_args.entries.len() > 0 {
                            // majority of server has reply success
                            *match_index_i.lock().unwrap() = send_entry_index;
                            *next_inedx_i.lock().unwrap() += 1;
                            myprintln!("{} update next index to {}", i, *next_inedx_i.lock().unwrap());
                        } else if !reply.success {
                            *match_index_i.lock().unwrap() = send_entry_index-1;
                            *next_inedx_i.lock().unwrap() -= 1;
                            myprintln!("{} rollback next index to {}", i, *next_inedx_i.lock().unwrap());
                        }
                        if reply.term > term{
                            myprintln!("{} become follower because of lower term", me);
                            *next_role.lock().unwrap() = Role::FOLLOWER;
                        }
                        //tx.send(reply);
                    },
                    Err(e) => {
                        myprintln!("append failed because {:?}", e);                            
                    },
                }
            });
        }
        //drop(tx);
        //drop(tx);
        /* for reply in rx.iter(){
            myprintln!("get reply");
            if (self.last_applied != self.last_index) && 
                (success_count.load(Ordering::SeqCst) >= ((self.peer_num() + 1) / 2) as usize){
                myprintln!("{} apply log", self.me);
                self.apply_ch.send(ApplyMsg{
                    command_valid: true,
                    command: self.log[self.last_index as usize].entry.clone(),
                    command_index: self.last_index,
                }).unwrap();
                self.commit_index = self.last_index;
                self.last_applied = self.last_index;
            }
        } */
        //finish_append.send(1).unwrap();
        myprintln!("{} end this heartbeat", self.me);
    }

    fn apply_log(&mut self){
        if let Some(match_index) = &self.match_index{
            let mut matched_count = 1;
            for index in match_index{
                if *index.lock().unwrap() == self.last_index{
                    matched_count += 1;
                }
            }
            if matched_count > self.peer_num() / 2{
                self.commit_index.store(self.last_index as usize, Ordering::SeqCst);
            }
        }   
        if self.commit_index.load(Ordering::SeqCst) > self.last_applied as usize{
            myprintln!("{} apply a log", self.me);
            let msg = ApplyMsg{
                command_valid: true,
                command: self.log[self.commit_index.load(Ordering::SeqCst)].entry.clone(),
                command_index: self.commit_index.load(Ordering::SeqCst) as u64,
            };
            let tx = self.apply_ch.clone();
            thread::spawn(move || {
                tx.send(msg)
                    .map(move |_| println!("apply a log index"))
                    .map_err(|e| eprintln!("error = {:?}", e))
                    .unwrap();
            }); 
            self.last_applied = self.commit_index.load(Ordering::SeqCst) as u64;    
        }
    }

    /* fn commit_last_log(&mut self){
        self.commit_index = self.last_index;
    } */

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
#[derive(Copy, Clone, PartialEq, Debug)]
enum Role{
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
            loop{
                let role;{
                    role = raft.lock().unwrap().role;
                }
                let me;{
                    me = raft.lock().unwrap().me;
                }
                match role{
                    Role::FOLLOWER => {
                        // start a timeout and if time out then become candidate
                        let rand_time = rand::thread_rng().gen_range(200, 500);
                        myprintln!("{} rand sleep {}", me, rand_time);
                        match rx.recv_timeout(time::Duration::from_millis(rand_time)){
                            Ok(_) => {
                                myprintln!("{} refresh timeout", me);
                            },
                            // wait util timeout and transfer to candidate
                            Err(e) => {
                                myprintln!("{} transfer to CANDIDATE becaues {:?}", me, e);
                                raft.lock().unwrap().transfer_state(Role::CANDIDATE);
                            },
                        } 
                    },
                    Role::CANDIDATE => {
                        // start an election and request for vote
                        let peer_num;{
                            peer_num = raft.lock().unwrap().peer_num();
                        }
                        let vote_count;{
                            vote_count = raft.lock().unwrap().vote_count.clone();
                        }
                        let vote_req;{
                            vote_req = raft.lock().unwrap().vote_req.clone();
                        }
                        //println!("main: got {} vote", vote_count.load(Ordering::SeqCst)); 
                        if (vote_req.load(Ordering::SeqCst) as u64 == (peer_num - 1)) &&
                           ((vote_count.load(Ordering::SeqCst) as u64) < (peer_num + 1) / 2){
                            // finish all the vote req and vote is not enough
                            raft.lock().unwrap().transfer_state(Role::FOLLOWER);
                        }
                        if vote_count.load(Ordering::SeqCst) as u64 >= (peer_num + 1) / 2 {
                            raft.lock().unwrap().transfer_state(Role::LEADER);
                        }
                    },
                    Role::LEADER => {
                        // send heartbeat periodly
                        //let (tx, rx) = channel();
                        thread::sleep(time::Duration::from_millis(20));
                        {
                            let mut raft = raft.lock().unwrap(); 
                            let next_role = *raft.next_role.lock().unwrap();
                            myprintln!("{} next role is {:?}", raft.me, next_role);
                            if next_role != raft.role{
                                raft.transfer_state(next_role);
                                continue;
                            }
                        }
                        {
                            myprintln!("{} start heartbeat", me);
                            raft.lock().unwrap().start_append_entry();
                        }
                        
                        
                        //rx.recv().unwrap();
                    },
                } 
                raft.lock().unwrap().apply_log();
                if shutdown.load(Ordering::SeqCst) {break;}
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
        self.raft.lock().unwrap().persist();
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let me;
        {
            me = self.raft.lock().unwrap().me;
        }
        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };
        self.timeout_tx.send(1).unwrap();
        {
            let mut raft = self.raft.lock().unwrap();
            let term = raft.state.term();
            myprintln!("{}[term {}] get vote req from {}[term {}]", me, term, args.candidate_id, args.term);
            let vote_for = raft.vote_for;
            if (term < args.term/* < or <=?*/) || ((term == args.term) && (raft.vote_for == -1)){
                // reset timeout
                raft.state = Arc::new({
                    State{
                        term: args.term,
                        is_leader: false,
                    }
                });
                // record vote 
                if args.last_log_index < raft.last_index{
                    myprintln!("{} reply false because of higher index[{}] > {}", 
                                                                            me, 
                                                                            raft.last_index, 
                                                                            args.last_log_index);
                    raft.vote_for = -1;
                    reply.term = term;
                    reply.vote_granted = false;
                }else{
                    raft.vote_for = args.candidate_id as i32;
                    reply.term = term;
                    reply.vote_granted = true;
                }
                
            }
            myprintln!("{} reply {}", me, reply.vote_granted);
            Box::new(futures::future::result(Ok(reply)))
        }
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let me;
        {
            me = self.raft.lock().unwrap().me;
        }
        if args.entries.len() == 0{
            myprintln!("{} get heartbeat from {}", me, args.leader_id);
        }else{
            myprintln!("{} get log from {}", me, args.leader_id);
        }
        self.timeout_tx.send(1).unwrap();
        let mut reply = AppendEntriesReply {
            term: 0,
            success: false,
        };
        {
            let raft = self.raft.lock().unwrap();
            reply.term = raft.state.term();
        }
        let role;{
            role = self.raft.lock().unwrap().role;
        }
        match role{
            Role::FOLLOWER =>{
                
            },

            Role::CANDIDATE =>{
                if args.term >= reply.term{
                    self.raft.lock().unwrap().transfer_state(Role::FOLLOWER);
                }
            },

            Role::LEADER =>{
                if args.term > reply.term{
                    myprintln!("leader {} becomes follower because term", me);
                    self.raft.lock().unwrap().transfer_state(Role::FOLLOWER);
                }
            },
        }
        let prev_log_term;{
            let raft = self.raft.lock().unwrap();
            prev_log_term = if (raft.log.len() - 1) > 0 {
                raft.log[raft.last_index as usize].term
            }else{
                raft.state.term()
            }
        }
        if args.term < reply.term{
            myprintln!("{} reply false for {} because term [{} != {}]", me, args.leader_id, reply.term, args.term);
            Box::new(futures::future::result(Ok(reply)))
        }else if prev_log_term != args.prev_log_term{
            myprintln!("{} reply false for {} because prev log term [{} != {}]", 
                me, 
                args.leader_id, 
                prev_log_term, 
                args.prev_log_term);
            Box::new(futures::future::result(Ok(reply)))
        }else {
            let mut raft = self.raft.lock().unwrap();
             if args.leader_commit > raft.commit_index.load(Ordering::SeqCst) as u64{
                 myprintln!("{} apply log", me);
                 // TODO
             }
            if raft.leader_id != args.leader_id as usize {raft.leader_id = args.leader_id as usize};
            if raft.state.term < args.term {
                raft.state = Arc::new(State {
                    term: args.term,
                    is_leader: false,
                });
            }
            if args.entries.len() > 0 {
                myprintln!("{} append log, last index[{}], log len[{}], commit_index[{}]",
                                me,
                                raft.last_index,
                                raft.log.len(),
                                raft.commit_index.load(Ordering::SeqCst));
                let last_index = raft.last_index;
                raft.log.reserve(1);
                raft.log.insert((last_index + 1) as usize, 
                                LogEntry::from_data(args.term, last_index + 1, &args.entries));
                raft.last_index += 1; 
                raft.commit_index.store(raft.last_index as usize, Ordering::SeqCst);               
            }
                
            reply.success = true;
            myprintln!("reply true for {}", args.leader_id);
            Box::new(futures::future::result(Ok(reply)))
        }
    }
}
