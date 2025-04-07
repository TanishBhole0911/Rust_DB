//// filepath: c:\Users\srija\Documents\GitHub\Rust_DB\testing\src\commands\db.rs
use crate::table::table::Table;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter, BufRead};
use thiserror::Error;
use log::{info, error};
use serde_json;
use std::fs::OpenOptions;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Table '{0}' already exists.")]
    TableAlreadyExists(String),
    #[error("Table '{0}' does not exist.")]
    TableDoesNotExist(String),
    #[error("Row '{0}' does not exist in table '{1}'.")]
    RowDoesNotExist(String, String),
    #[error("Row '{0}' not found in table '{1}'.")]
    RowNotFound(String, String),
    #[error("Error creating file '{0}': {1}")]
    FileCreationError(String, String),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

pub struct Database {
    pub tables: HashMap<String, Table>,
    pub operations_since_save: usize,
    pub save_threshold: usize,
    pub wal: Vec<String>,
    pub wal_file: String,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
            operations_since_save: 0,
            save_threshold: 5,
            wal: Vec::new(),
            wal_file: "wal.log".to_string(),
        }
    }

    pub fn check_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    // Create table: update in-memory state and log to WAL.
    pub fn create_table(&mut self, table_name: &str) -> Result<String> {
        if self.check_table(table_name) {
            error!("Table '{}' already exists.", table_name);
            Err(DatabaseError::TableAlreadyExists(table_name.to_string()))
        } else {
            // Update in-memory table immediately.
            self.tables.insert(table_name.to_string(), Table::new());
            // Log the operation
            let op = format!("create_table:{}", table_name);
            self.wal.push(op.clone());
            println!("Table '{}' created and logged to WAL", table_name);
            Ok(table_name.to_string())
        }
    }

    // Add a column: log and update in-memory.
    pub fn add_column(&mut self, table_name: &str, column_name: &str) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.add_column(column_name);
            let op = format!("add_column:{}:{}", table_name, column_name);
            self.wal.push(op.clone());
            println!("Column '{}' added to table '{}' and logged to WAL", column_name, table_name);
            Ok(vec![column_name.to_string(), table_name.to_string()])
        } else {
            error!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // Get row from table.
    pub fn get_row(&self, table_name: &str, row_id: &str) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get(table_name) {
            if let Some(row) = table.get_row(row_id) {
                println!("Row '{}': {:?}", row_id, row);
                let row_string = format!("{:?}", row);
                Ok(vec![row_id.to_string(), row_string])
            } else {
                error!("Row '{}' does not exist in '{}'.", row_id, table_name);
                Err(DatabaseError::RowDoesNotExist(row_id.to_string(), table_name.to_string()))
            }
        } else {
            error!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // Insert row: update in-memory table and log the operation.
    pub fn insert_row(&mut self, table_name: &str, row_id: &str, data: HashMap<String, String>) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get_mut(table_name) {
            // Update the table in memory.
            table.insert_row(row_id, data.clone());
            let op = format!(
                "insert_row:{}:{}:{}",
                table_name,
                row_id,
                serde_json::to_string(&data).unwrap()
            );
            self.wal.push(op);
            println!("Inserted row '{}' in table '{}' and logged to WAL", row_id, table_name);
            
            self.operations_since_save += 1;
            if self.operations_since_save >= self.save_threshold {
                let file_name = format!("{}.csv", table_name);
                if let Err(e) = self.save_table(table_name, &file_name) {
                    error!("Failed to save table '{}': {}", table_name, e);
                }
                self.operations_since_save = 0;
            }
            Ok(vec![row_id.to_string(), table_name.to_string()])
        } else {
            println!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // Save the table to a CSV file.
    pub fn save_table(&self, table_name: &str, file_name: &str) -> Result<Vec<String>> {
        match self.tables.get(table_name) {
            Some(table) => {
                let mut columns_in_order: Vec<_> = table.columns.iter().cloned().collect();
                columns_in_order.sort();
                let file_result = File::create(file_name);
                match file_result {
                    Ok(file) => {
                        let mut writer = BufWriter::new(file);
                        let header = {
                            let mut hdr = vec!["row_id".to_string()];
                            hdr.extend(columns_in_order.iter().cloned());
                            hdr.join(",")
                        };
                        writeln!(writer, "{}", header).unwrap();
                        for (row_id, row_data) in &table.rows {
                            let mut row_vec = vec![row_id.clone()];
                            for col in &columns_in_order {
                                row_vec.push(row_data.get(col).cloned().unwrap_or_default());
                            }
                            writeln!(writer, "{}", row_vec.join(",")).unwrap();
                        }
                        println!("Table '{}' saved to '{}'.", table_name, file_name);
                        Ok(vec![table_name.to_string(), file_name.to_string()])
                    }
                    Err(e) => {
                        error!("Error creating file '{}': {}", file_name, e);
                        Err(DatabaseError::FileCreationError(file_name.to_string(), e.to_string()))
                    }
                }
            }
            None => {
                error!("Table '{}' does not exist.", table_name);
                Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
            }
        }
    }

    // --- WAL functions ---
    // flush_wal() replays all in‑memory operations.
    pub fn flush_wal(&mut self) -> Result<()> {
        for entry in &self.wal {
            let parts: Vec<&str> = entry.split(':').collect();
            match parts[0] {
                "create_table" => {
                    // Already applied during create_table.
                    println!("Replay: Table '{}' exists.", parts[1]);
                }
                "add_column" => {
                    if let Some(table) = self.tables.get_mut(parts[1]) {
                        table.add_column(parts[2]);
                        println!("Replay: Column '{}' added to table '{}'.", parts[2], parts[1]);
                    }
                }
                "insert_row" => {
                    let table_name = parts[1];
                    let row_id = parts[2];
                    match serde_json::from_str::<HashMap<String, String>>(parts[3]) {
                        Ok(data) => {
                            if let Some(table) = self.tables.get_mut(table_name) {
                                table.insert_row(row_id, data);
                                println!("Replay: Row '{}' inserted into table '{}'.", row_id, table_name);
                            }
                        }
                        Err(e) => {
                            error!("Failed to deserialize row data for table '{}': {}", table_name, e);
                        }
                    }
                }
                _ => {
                    println!("Unknown WAL entry: {}", entry);
                }
            }
        }
        Ok(())
    }

    // persist_wal() writes the in‑memory WAL to disk in append mode.
    pub fn persist_wal(&self) -> Result<()> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.wal_file)
            .map_err(|err| DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string()))?;
        let mut writer = BufWriter::new(file);
        for entry in &self.wal {
            writeln!(writer, "{}", entry)
                .map_err(|err| DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string()))?;
        }
        writer.flush().unwrap();
        println!("WAL persisted to {}", self.wal_file);
        Ok(())
    }

    // load_wal() reads existing WAL operations from disk.
    pub fn load_wal(&mut self) -> Result<()> {
        let file = File::open(&self.wal_file);
        if let Ok(file) = file {
            let reader = std::io::BufReader::new(file);
            for line in reader.lines() {
                if let Ok(entry) = line {
                    self.wal.push(entry);
                }
            }
            // Replay loaded WAL to update in‑memory state.
            self.flush_wal()?;
        } else {
            println!("No WAL file found. Starting fresh.");
        }
        Ok(())
    }

    // clear_wal() clears both the in‑memory WAL and truncates the WAL file.
    pub fn clear_wal(&mut self) -> Result<()> {
        self.wal.clear();
        File::create(&self.wal_file)
            .map_err(|err| DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string()))?;
        println!("WAL cleared.");
        Ok(())
    }

    // replay_wal() simply flushes the WAL to replay its operations.
    pub fn replay_wal(&mut self) -> Result<()> {
        self.flush_wal()?;
        Ok(())
    }
}