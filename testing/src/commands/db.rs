use crate::table::table::Table;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter, BufRead};
use thiserror::Error;
use log::{info, error};
use serde_json;

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

    pub fn create_table(&mut self, table_name: &str) -> Result<String> {
        if self.check_table(table_name) {
            error!("Table '{}' already exists.", table_name);
            Err(DatabaseError::TableAlreadyExists(table_name.to_string()))
        } else {
            // self.tables.insert(table_name.to_string(), Table::new());
            self.wal.push(format!("create_table:{}", table_name));
            info!("Table '{}' created To WAL", table_name);
            Ok(table_name.to_string())
        }
    }

    pub fn add_column(&mut self, table_name: &str, column_name: &str) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get_mut(table_name) {
            // table.add_column(column_name);
            self.wal.push(format!("add_column:{}:{}", table_name, column_name));
            info!("Added column '{}' to table '{}' to WAL", column_name, table_name);
            Ok(vec![column_name.to_string(), table_name.to_string()])
        } else {
            error!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    pub fn get_row(&self, table_name: &str, row_id: &str) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get(table_name) {
            if let Some(row) = table.get_row(row_id) {
                info!("Row '{}': {:?}", row_id, row);
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

    pub fn insert_row(&mut self, table_name: &str, row_id: &str, data: HashMap<String, String>)->Result<Vec<String>> {
        if let Some(table) = self.tables.get_mut(table_name) {
            // table.insert_row(row_id, data);
            self.wal.push(format!("insert_row:{}:{}:{:?}", table_name, row_id, data));
            println!("Inserted row '{}' in table '{}' to WAL", row_id, table_name);
            
            // Increment the operation count and check if we need to auto-save
            self.operations_since_save += 1;
            if self.operations_since_save >= self.save_threshold {
                // Automatically save to a file named after table_name or your choice
                let file_name = format!("{}.csv", table_name);
                if let Err(e) = self.save_table(table_name, &file_name) {
                    error!("Failed to save table '{}': {}", table_name, e);
                }
                // Reset the counter
                self.operations_since_save = 0;
                
            }
            Ok(vec![row_id.to_string(), table_name.to_string()])
        } else {
            println!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    pub fn delete_row(&mut self, table_name: &str, row_id: &str) -> Result<Vec<String>> {
        if let Some(_) = self.tables.get_mut(table_name) {
            self.wal.push(format!("delete_row:{}:{}", table_name, row_id)); // Log to WAL
            info!("Row '{}' deleted from '{}'.", row_id, table_name);
            
            // Increment the operation count and check if we need to auto-save
            self.operations_since_save += 1;
            if self.operations_since_save >= self.save_threshold {
                self.flush_wal()?; // Flush WAL to main storage
                self.operations_since_save = 0; // Reset the counter
            }
            Ok(vec![row_id.to_string(), table_name.to_string()])
        } else {
            error!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    pub fn get_table(&self, table_name: &str) -> Result<&Table> {
        self.tables.get(table_name).ok_or_else(|| {
            error!("Table '{}' does not exist.", table_name);
            DatabaseError::TableDoesNotExist(table_name.to_string())
        })
    }

    pub fn flush_wal(&mut self) -> Result<()> {
        for entry in &self.wal {
            let parts: Vec<&str> = entry.split(':').collect();
            match parts[0] {
                "create_table" => {
                    self.tables.insert(parts[1].to_string(), Table::new());
                }
                "add_column" => {
                    if let Some(table) = self.tables.get_mut(parts[1]) {
                        table.add_column(parts[2]);
                    }
                }
                "insert_row" => {
                    let table_name = parts[1];
                    let row_id = parts[2];
                    let data: HashMap<String, String> = serde_json::from_str(parts[3]).unwrap();
                    if let Some(table) = self.tables.get_mut(table_name) {
                        table.insert_row(row_id, data);
                    }
                }
                "delete_row" => {
                    let table_name = parts[1];
                    let row_id = parts[2];
                    if let Some(table) = self.tables.get_mut(table_name) {
                        table.delete_row(row_id);
                    }
                }
                _ => {}
            }
        }
        self.wal.clear(); // Clear the WAL after flushing
        Ok(())
    }

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

                        info!("Table '{}' saved to '{}'.", table_name, file_name);
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


    // WAL FUNCTIONS -------------
    pub fn persist_wal(&self) -> Result<()> {
        let file = match File::create(&self.wal_file) {
            Ok(it) => it,
            Err(err) => return Err(DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string())),
        };
        let mut writer = BufWriter::new(file);
        for entry in &self.wal {
            match writeln!(writer, "{}", entry) {
                Ok(it) => it,
                Err(err) => return Err(DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string())),
            };
        }
        Ok(())
    }

    pub fn load_wal(&mut self) -> Result<()> {
        let file = File::open(&self.wal_file);
        if let Ok(file) = file {
            let reader = std::io::BufReader::new(file);
            for line in reader.lines() {
                if let Ok(entry) = line {
                    self.wal.push(entry);
                }
            }
            self.flush_wal()?; // Replay the WAL to apply changes
        } else {
            info!("No WAL file found. Starting fresh.");
        }
        Ok(())
    }

    pub fn clear_wal(&mut self) -> Result<()> {
        self.wal.clear();
        let _ = match File::create(&self.wal_file) {
            Ok(it) => it,
            Err(err) => return Err(DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string())),
        }; // Overwrite the WAL file with an empty file
        Ok(())
    }

    pub fn replay_wal(&mut self) -> Result<()>{
        self.flush_wal()?;
        // self.wal.clear(); // Clear the WAL after replaying
        Ok(())
    }


}