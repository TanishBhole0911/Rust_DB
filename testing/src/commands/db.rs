use crate::table::table::Table;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter};
use thiserror::Error;
use log::{info, error};

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
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
            operations_since_save: 0,
            save_threshold: 5,
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
            self.tables.insert(table_name.to_string(), Table::new());
            info!("Table '{}' created.", table_name);
            Ok(table_name.to_string())
        }
    }

    pub fn add_column(&mut self, table_name: &str, column_name: &str) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.add_column(column_name);
            info!("Added column '{}' to table '{}'.", column_name, table_name);
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

    pub fn delete_row(&mut self, table_name: &str, row_id: &str) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get_mut(table_name) {
            if table.delete_row(row_id) {
                info!("Row '{}' deleted from '{}'.", row_id, table_name);
                Ok(vec![row_id.to_string(), table_name.to_string()])
            } else {
                error!("Row '{}' not found in '{}'.", row_id, table_name);
                Err(DatabaseError::RowNotFound(row_id.to_string(), table_name.to_string()))
            }
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
}