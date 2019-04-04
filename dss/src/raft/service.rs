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
    term: u64,
    // cadidate requesting vote
    candidateId: u64,
    // index of candidate's last log entry
    lastLogIndex: u64,
    // term of candidate's last log entry 
    lastLogTerm: u64,
}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    // current term, for candidate to update itself
    term: u64,
    // candidate recieve vote when it is ture
    voteGranted: bool
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesArgs{
    // leader's term
    term: u64,
    // when client connect to follower 
    // then it will be redirected to leader by this id
    leaderId: u64,
    // index of log entry immidiately preceding new ones
    prevLogIndex: u64,
    // term of prevLogIndex entry
    prevLogTerm: u64,
    // log entry or null for heart beat
    entries: Option<String>,
    // leader's commit index
    leaderCommit: u64,
}

#[derive(CLone, PartialEq, Message)]
pub struct AppendEntriesReply{
    // current term
    term: u64,
    // if follower contained entry 
    // matching prevLogIndex and prevLogTerm
    success: bool,
}