//use crate::service::{TSOClient, TransactionClient};
use crate::service::*;
use crate::msg::*;
use labrpc::*;
use futures::Future;
use std::time::Duration;
use std::thread;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    writes: Vec<Write>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            writes: vec![],
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let req = TimestampRequest{};
        let mut back_off_time = BACKOFF_TIME_MS;
        let mut res: Result<u64> = Err(Error::Timeout);
        for _ in 0..RETRY_TIMES{
            match self.tso_client.get_timestamp(&req).wait(){
                Ok(reply) => {
                    //println!("get a timestamp {}", reply.ts);
                    res = Ok(reply.ts);
                    break;
                },
                Err(e) => {
                    //println!("get a timestamp failed");
                    res = Err(e)
                },
            }
            // sleep back_off_time and retry
            thread::sleep(Duration::from_millis(back_off_time));
            back_off_time *= 2;
        }
        res
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // 1. get a start_ts from TSOClient
        // 2. init writes
        if let Ok(ts) = self.get_timestamp(){
            self.start_ts = ts;
            self.writes.clear();
        }
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        let tso_req = TimestampRequest{};
        let req = GetRequest{
            key,
            start_ts: self.start_ts,
        };
        match self.txn_client.get(&req).wait(){
            Ok(reply) => {
                if reply.key_is_locked{
                    thread::sleep(Duration::from_millis(100 as u64));
                    match self.txn_client.get(&req).wait(){
                        Ok(reply) =>{
                            if reply.found{
                                return Ok(reply.value);
                            }else{
                                return Ok(Vec::new());
                            }
                        },
                        Err(e) => return Err(e),
                    }
                }else{
                    if reply.found{
                        Ok(reply.value)
                    }else{
                        Ok(Vec::new())
                    }
                }
            },
            Err(e) => Err(e),
        }
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // cache key-value pair in the memory
        self.writes.push(Write(key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // 1.prewrite(primary key)
        // 1.1 choose a primary key
        if self.writes.len() == 0 {
            return Ok(true);
        }
        let num_writes = self.writes.len();
        let primary = &self.writes[0];
        let mut secondary: Vec<Write> = Vec::new();
        for kv in self.writes.iter()
            .enumerate()
            .filter(|&(i, _)| i != 0)
            .map(|(_, v)| v){
            secondary.push(kv.clone());
        }

        let primary_kv = KeyValue{
            key: primary.0.clone(),
            value: primary.1.clone(),
        };
        let primary_prewrite_request = PrewriteRequest{
            kvs: vec![primary_kv],
            primary_key: primary.0.clone(),
            start_ts: self.start_ts,
        };
        match self.txn_client.prewrite(&primary_prewrite_request).wait(){
            Ok(reply) => {
              if !reply.success{
                  return Ok(false);
              }
            },
            Err(_) => {return Ok(false);}
        }

        // 2.prewrite(secondary key)
        let mut vec_kvs = Vec::new();
        for kvs in &secondary{
            vec_kvs.push(KeyValue{
                key: kvs.0.clone(),
                value: kvs.1.clone(),
            });
        }
        let secondary_prewrite_request = PrewriteRequest{
            kvs: vec_kvs,
            primary_key: primary.0.clone(),
            start_ts: self.start_ts,
        };

        match self.txn_client.prewrite(&secondary_prewrite_request).wait(){
            Ok(reply) =>{
                if !reply.success {return Ok(false);}
            },
            Err(_) => {return Ok(false);}
        }
        // 3.commit(primary key) and return false if failed
        // 3.1 get commit ts
        let get_timestamp_req = TimestampRequest{};
        let commit_ts: u64;
        match self.tso_client.get_timestamp(&get_timestamp_req).wait(){
            Ok(reply) => commit_ts = reply.ts,
            Err(_) => panic!("failed to get timestamp"),
        }
        // for primary key
        let start_ts = self.start_ts;
        let primary_commit_req = CommitRequest{
            is_primary: true,
            commit_ts,
            start_ts,
            keys: vec![primary.0.clone()],
        };
        match self.txn_client.commit(&primary_commit_req).wait(){
            Ok(reply) =>{
                if !reply.success {return Ok(false);}
            },
            Err(e) => {return Err(e);}
        }
        // 4.commit(secondary key)
        let keys = secondary.iter().map(|v| v.0.clone()).collect();
        let secondary_commit_req = CommitRequest{
            is_primary: false,
            commit_ts,
            start_ts,
            keys,
        };
        self.txn_client.spawn(self.txn_client
            .commit(&secondary_commit_req)
            .then(|_|{
                Ok(())
            }));
        //self.txn_client.commit(&secondary_commit_req).wait();
        Ok(true)
    }
}
