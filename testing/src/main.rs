//// filepath: c:\Users\srija\Documents\GitHub\Rust_DB\testing\src\main.rs
#[warn(unused_imports)]
use std::fs;
use env_logger;
pub mod table;

mod commands;
const FOLDER_PATH: &str = "./src/commands";
use commands::{command1, command2, db, walengine};


use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

fn get_command_names() -> Vec<String> {
    let folder_path = FOLDER_PATH;
    let mut files = vec![];
    if let Ok(entries) = fs::read_dir(folder_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("rs")) &&
               path.file_name() != Some(std::ffi::OsStr::new("mod.rs")) {
                if let Some(file_name) = path.file_name().and_then(|f| f.to_str()) {
                    println!("{:?}", file_name);
                    files.push(file_name.split('.').next().unwrap().to_string());
                }
            }
        }
    } else {
        eprintln!("Error reading directory");
    }
    println!("{:?}", files);
    files
}

fn main() {
    env_logger::init();

    // Initialize the database wrapped in Arc<Mutex<>>
    let db = Arc::new(Mutex::new(db::Database::new()));
    let running = Arc::new(AtomicBool::new(true));

    // Load the WAL at startup
    {
        let mut db_lock = db.lock().unwrap();
        if let Err(e) = db_lock.load_wal() {
            eprintln!("Failed to load WAL: {}", e);
        }
        if let Err(e) = db_lock.flush_wal() {
            eprintln!("Failed to flush WAL: {}", e);
        }
    }

    // Start the WAL engine to persist/replay WAL periodically
    let wal_engine = walengine::WalEngine::new(Arc::clone(&db), Duration::from_secs(10));
    thread::spawn(move || wal_engine.start());

    // Simulate database operations
    {
        let mut db_lock = db.lock().unwrap();
        // db_lock.create_table("users").unwrap();
        // db_lock.flush_wal().unwrap();
    
        // db_lock.add_column("users", "name").unwrap();
        // db_lock.add_column("users", "age").unwrap();
        // db_lock.add_column("users", "email").unwrap();
        
        let mut row_data = std::collections::HashMap::new();
        row_data.insert("name".to_string(), "yes".to_string());
        row_data.insert("age".to_string(), "100".to_string());
        row_data.insert("email".to_string(), "xyz@.com".to_string());
        // db_lock.insert_row("users", "1", row_data).unwrap();
        
        // db_lock.save_table("users", "users.csv").unwrap();

        db_lock.update_row("users", "4", "age", "10").unwrap();
        db_lock.update_row("users", "2", "email", "y@.com").unwrap();


        match db_lock.get_row("users", "1") {
            Ok(row) => println!("Row: {:?}", row),
            Err(e) => eprintln!("Error: {}", e),
        }

        match db_lock.get_table("users") {
            Ok(table) => println!("Table: {}", table),
            Err(e) => eprintln!("Error: {}", e),
        }

        match db_lock.search_rows_by_condition_in_table("users", "age < 10") {
            Ok(rows) => println!("Rows: {:?}", rows),
            Err(e) => eprintln!("Error: {}", e),
        }

        match db_lock.find_rows_by_value_in_table("users", "age", "5", false) {
            Ok(rows) => println!("Rows: {:?}", rows),
            Err(e) => eprintln!("Error: {}", e),
        }
        // Optionally, perform a manual commit here if needed:
        // db_lock.flush_wal().unwrap();
        db_lock.commit_wal().unwrap();
    }

    // Run for a finite duration then exit.
    thread::sleep(Duration::from_secs(60));
    running.store(false, Ordering::SeqCst);
    println!("Shutting down.");
}