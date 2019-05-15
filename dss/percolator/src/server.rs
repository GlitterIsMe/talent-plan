use crate::msg::*;
use crate::service::*;
use crate::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::ops::Bound::Included;

use labrpc::RpcFuture;


use chrono::prelude::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // need nothing
}

impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    fn get_timestamp(&self, _: TimestampRequest) -> RpcFuture<TimestampResponse> {
        // Your code here.
        let dt = Utc::now();
        Box::new(futures::future::result(Ok(TimestampResponse {
            ts: dt.timestamp_nanos() as u64,
        })))
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: &Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let start_ts = match ts_start_inclusive {
            Some(ts) => ts,
            None => 0,
        };

        let end_ts = match ts_end_inclusive {
            Some(ts) => ts,
            None => u64::max_value(),
        };

        let col: &BTreeMap<Key, Value> = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };

        for kv in col.range((Included(&(key.clone(), start_ts)), Included(&(key.clone(), end_ts)))).rev() {
            return Some(kv);
        }
        None
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let col: &mut BTreeMap<Key, Value> = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        col.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: &Vec<u8>, column: Column, start_ts: u64) {
        let col: &mut BTreeMap<Key, Value> = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        col.remove(&(key.to_vec(), start_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        //println!("Get {:?}@{}", req.key, req.start_ts);
        let mut reply = GetResponse {
            found: false,
            value: vec![],
            key_is_locked: false,
        };
        loop {
            let snapshot = self.get_snapshot();
            match snapshot.read(&req.key, Column::Lock, None, Some(req.start_ts)) {
                Some((key, value)) => {
                    // continue wait
                    // start_ts and key is the start_ts and key of the lock
                    // req.key == key.0
                    //println!("get lock {:?}@{:?}", key.0, key.1);
                    if !self.back_off_maybe_clean_up_lock(key.1, req.key.clone()){
                        reply.key_is_locked = true;
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                }
                None => {
                    break;
                }
            }
        }

        let snapshot = self.get_snapshot();
        match snapshot.read(&req.key, Column::Write, None, Some(req.start_ts)) {
            Some(res) => {
                match res.1 {
                    Value::Vector(_) => {
                        panic!("this should be a timestamp");
                    }
                    Value::Timestamp(ts) => {
                        match snapshot.read(&req.key,  Column::Data,  Some(*ts),  Some(*ts), ) {
                            Some(kv) => {
                                match kv.1 {
                                    Value::Timestamp(_) => panic!("this should be a value"),
                                    Value::Vector(v) => {
                                        reply.value = v.to_vec();
                                        reply.found = true;
                                        return Box::new(futures::future::result(Ok(reply)));
                                    }
                                }
                            }
                            None => {
                                return Box::new(futures::future::result(Ok(reply)));
                            }
                        }
                    }
                }
            }
            None => {
                return Box::new(futures::future::result(Ok(reply)));
            }
        }
    }

    // example prewrite RPC handler.
    fn prewrite(&self, req: PrewriteRequest) -> RpcFuture<PrewriteResponse> {
        // 1.read from write@[start_ts, ∞]
        // if there has a commit record then return false
        //println!("PreWrite @{}", req.start_ts);
        let mut reply = PrewriteResponse { success: false };
        for kv in req.kvs {
            {
                let data = self.data
                    .lock()
                    .unwrap();
                let check_commit = data.read(
                    &kv.key,
                    Column::Write,
                    Some(req.start_ts),
                    None);
                match check_commit {
                    Some(res) => {
                        //println!("get a commit after start_ts {:?}@{:?}", (res.0).0, (res.0).1);
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                    None => (),
                }
            }

            // 2.read from lock@[0, ∞]
            // if there is a lock then return false
            {
                let data = self.data
                    .lock()
                    .unwrap();
                let check_commit = data.read(
                    &kv.key,
                    Column::Lock,
                    None,
                    None);
                match check_commit {
                    Some(res) => {
                        //println!("get a lock{:?}@{:?}", (res.0).0, (res.0).1);
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                    None => (),
                }
            }
            // 3.write lock <<key, start_ts>, primary_key>
            // 4.write data <<key, start_ts>, value>
            {
                let mut data = self.data.lock().unwrap();
                data.write(kv.key.clone(), Column::Lock, req.start_ts, Value::Vector(req.primary_key.clone()));
                data.write(kv.key, Column::Data, req.start_ts, Value::Vector(kv.value));
            }
        }

        // 5.return success
        reply.success = true;
        Box::new(futures::future::result(Ok(reply)))
    }

    // example commit RPC handler.
    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        // 对于commit，只做commit的逻辑，其他逻辑交给client
        // commit要幂等，比如重复的commit，保证不同的commit的操作结果一致（容错）
        // 健壮性的要求，客户端的错误及时返回错误，不能将错误结果写入使得错误叠加
        // 1.for primary
        // read key from lock@start_ts
        // if no lock return false
        // else write ((key, commit_ts), start_ts) and then remove lock((key, start_ts))
        //println!("Commit @{}", req.commit_ts);
        let mut reply = CommitResponse { success: false };
        let snap = self.get_snapshot();
        if req.is_primary {
            match snap.read(&req.keys[0], Column::Lock, Some(req.start_ts), Some(req.start_ts)) {
                Some(_) => (),
                None => {
                    //println!("dont't get a lock of key[{:?}]@[{}]", req.keys[0], req.start_ts);
                    reply.success = false;
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            let mut data = self.data.lock().unwrap();
            data.erase(&req.keys[0], Column::Lock, req.start_ts);
            data.write(req.keys[0].clone(), Column::Write, req.commit_ts, Value::Timestamp(req.start_ts));
        } else {
            // 2.for secondary
            let mut data = self.data.lock().unwrap();
            for key in req.keys {
                data.erase(&key, Column::Lock, req.start_ts);
                data.write(key, Column::Write, req.commit_ts, Value::Timestamp(req.start_ts));
            }
        }
        reply.success = true;
        Box::new(futures::future::result(Ok(reply)))
    }
}

impl MemoryStorage {
    fn get_snapshot(&self) -> KvTable {
        self.data.lock().unwrap().clone()
    }

    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) -> bool{
        // try to wait for the lock
        // 这个start_ts是读出来的key的ts
        //println!("try back_off or cleanup {:?}@{:?}", key, start_ts);
        let mut dt = Local::now();
        let ts_now = dt.timestamp_nanos();
        let old_time = Duration::from_nanos(start_ts).as_nanos();
        let new_time = Duration::from_nanos(ts_now as u64).as_nanos();
        let time_duration = new_time - old_time;
        if time_duration < TTL as u128 {
            //println!("new_time{} - old_time{} = {}", new_time, old_time, time_duration as u64);
            // thread::sleep(Duration::from_nanos(TTL));
            return false;
            //println!("end sleep");
        } else {
            // if the lock execeeds ttl then clean_up
            // try to clean up
            //println!("execeeds ttl");//let data = self.data.lock().unwrap();
            let snapshot = self.get_snapshot();
            match snapshot.read(&key, Column::Lock, None, Some(start_ts)) {
                Some((key, value)) => {
                    //println!("try cleanup {:?}@{:?}", key.0, key.1);
                    match value {
                        Value::Vector(primary) => {
                            // value is the primary key
                            //println!("primary is {:?}", primary);
                            if key.0 == primary.to_vec() {
                                // 1.if this is a primary, then delete it
                                self.data.lock().unwrap().erase(primary, Column::Lock, key.1);
                            } else {
                                // 2.if this is a secondary
                                //  2.1 if primary has committed then roll-forward this lock
                                //  2.2 if primary has not committed then roll-back this lock
                                self.do_clean_up_secondary(start_ts, primary.to_vec(), key.0.clone());
                            }
                        }
                        Value::Timestamp(_) => {
                            panic!("not possible");
                        }
                    }
                }
                None => {
                    panic!("not possible");
                }
            }
            return true;
        }
    }

    fn do_clean_up_secondary(&self, start_ts: u64, primary_key: Vec<u8>, secondary_key: Vec<u8>) {
        let snapshot = self.get_snapshot();
        // 这里返回的是不可变引用，所以后面再对data进行修改的时候会出错
        match snapshot.read(&primary_key, Column::Write, Some(start_ts), None) {
            Some((key, value)) => {
                //println!("primary has committed and roll forward");
                // read a record indicate that primary has committed
                let key = key.clone();
                let secondary_start_ts: u64;
                if let Value::Timestamp(ts) = value{
                    secondary_start_ts = *ts;
                }else{
                    panic!("not posible");
                };
                let commit_ts = key.1;
                let mut data = self.data.lock().unwrap();
                data.erase(&secondary_key, Column::Lock, secondary_start_ts);
                //println!("erase lock{:?}@{:?}", secondary_key, start_ts);
                data.write(secondary_key, Column::Write, commit_ts, Value::Timestamp(secondary_start_ts));
            }
            None => {
                // no commit and cleanup
                //println!("primary has not committed and roll back");
                let mut data = self.data.lock().unwrap();
                data.erase(&secondary_key.clone(), Column::Lock, start_ts);
                //println!("erase lock{:?}@{:?}", secondary_key, start_ts);
            }
        }
    }
}
