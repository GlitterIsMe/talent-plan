labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);

        rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);

        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
    }
}
pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).

    // candidate's term
    #[prost(uint64, tag = "1")]
    pub term: u64,
    // cadidate requesting vote
    #[prost(uint64, tag = "2")]
    pub candidate_id: u64,
    // index of candidate's last log entry
    #[prost(uint64, tag = "3")]
    pub last_log_index: u64,
    // term of candidate's last log entry
    #[prost(uint64, tag = "4")]
    pub last_log_term: u64,
}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    // current term, for candidate to update itself
    #[prost(uint64, tag = "1")]
    pub term: u64,
    // candidate recieve vote when it is ture
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesArgs {
    // leader's term
    #[prost(uint64, tag = "1")]
    pub term: u64,
    // when client connect to follower
    // then it will be redirected to leader by this id
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    // index of log entry immidiately preceding new ones
    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,
    // term of prevLogIndex entry
    #[prost(uint64, tag = "4")]
    pub prev_log_term: u64,
    // log entry or null for heart beat
    #[prost(bytes, tag = "5")]
    pub entries: Vec<u8>,
    // entry's term
    #[prost(uint64, tag = "6")]
    pub entry_term: u64,
    // leader's commit index
    #[prost(uint64, tag = "7")]
    pub leader_commit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesReply {
    // current term
    #[prost(uint64, tag = "1")]
    pub term: u64,
    // if follower contained entry
    // matching prevLogIndex and prevLogTerm
    #[prost(bool, tag = "2")]
    pub success: bool,

    /*#[prost(uint64, tag = "3")]
    pub conflict_term: u64,

    #[prost(uint64, tag = "4")]
    pub conflict_index: u64,*/

}
