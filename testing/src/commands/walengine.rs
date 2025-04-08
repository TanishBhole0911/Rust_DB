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
                    // Persist the working WAL.
                    if let Err(e) = db.persist_wal() {
                        error!("Failed to persist WAL: {}", e);
                    } else {
                        info!("WAL persisted successfully.");
                    }
                    // Replay the WAL to update in-memory state.
                    if let Err(e) = db.replay_wal() {
                        error!("Failed to replay WAL: {}", e);
                    } else {
                        info!("WAL replayed successfully.");
                    }
                    // Now commit the WAL: archive the logged operations and clear the working log.
                    if let Err(e) = db.commit_wal() {
                        error!("Failed to commit WAL: {}", e);
                    } else {
                        info!("WAL commit completed.");
                    }
                }
                thread::sleep(interval);
            }
        });
    }
}