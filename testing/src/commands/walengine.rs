//// filepath: c:\Users\srija\Documents\GitHub\Rust_DB\testing\src\commands\walengine.rs
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use log::{info, error};
use super::db::Database;

pub struct WalEngine {
    db: Arc<Mutex<Database>>,
    interval: Duration,
}

impl WalEngine {
    pub fn new(db: Arc<Mutex<Database>>, interval: Duration) -> Self {
        WalEngine { db, interval }
    }

    pub fn start(&self) {
        let db_clone = Arc::clone(&self.db);
        let interval = self.interval;

        thread::spawn(move || {
            loop {
                {
                    let mut db = db_clone.lock().unwrap();

                    // Persist the WAL (append mode)
                    if let Err(e) = db.persist_wal() {
                        error!("Failed to persist WAL: {}", e);
                    } else {
                        info!("WAL persisted successfully.");
                    }

                    // Replay the WAL (update in‑memory state)
                    if let Err(e) = db.replay_wal() {
                        error!("Failed to replay WAL: {}", e);
                    } else {
                        info!("WAL replayed successfully.");
                    }

                    // Optionally, clear the in‑memory WAL if desired.
                    // Commented out here so that the log file keeps growing.
                    // if let Err(e) = db.clear_wal() {
                    //     error!("Failed to clear WAL: {}", e);
                    // } else {
                    //     info!("WAL cleared successfully.");
                    // }
                }
                thread::sleep(interval);
            }
        });
    }
}