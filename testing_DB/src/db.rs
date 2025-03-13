mod table;
use table::Table;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter};

pub struct Database {
    pub tables: HashMap<String, Table>,
    pub operations_since_save: usize,  // Track how many inserts/updates since last save
    pub save_threshold: usize,         // Automatically save after this many operations
}
impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
            operations_since_save: 0,
            save_threshold: 5, // Example threshold
        }
    }

    /// Create a table if it doesn’t exist.
    pub fn create_table(&mut self, table_name: &str) {
        if self.tables.contains_key(table_name) {
            println!("Table '{}' already exists.", table_name);
        } else {
            self.tables.insert(table_name.to_string(), Table::new());
            println!("Table '{}' created.", table_name);
        }
    }

    /// Add a column to an existing table.
    pub fn add_column(&mut self, table_name: &str, column_name: &str) {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.add_column(column_name);
            println!("Added column '{}' to table '{}'.", column_name, table_name);
        } else {
            println!("Table '{}' does not exist.", table_name);
        }
    }

    /// Insert or update data in a table.
    pub fn insert_row(&mut self, table_name: &str, row_id: &str, data: HashMap<String, String>) {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.insert_row(row_id, data);
            println!("Inserted/updated row '{}' in table '{}'.", row_id, table_name);

            // Increment the operation count and check if we need to auto-save
            self.operations_since_save += 1;
            if self.operations_since_save >= self.save_threshold {
                // Automatically save to a file named after table_name or your choice
                let file_name = format!("{}.csv", table_name);
                self.save_table(table_name, &file_name);
                // Reset the counter
                self.operations_since_save = 0;
            }
        } else {
            println!("Table '{}' does not exist.", table_name);
        }
    }

    /// Retrieve a row from a table.
    pub fn get_row(&self, table_name: &str, row_id: &str) {
        if let Some(table) = self.tables.get(table_name) {
            if let Some(row) = table.get_row(row_id) {
                println!("Row '{}': {:?}", row_id, row);
            } else {
                println!("Row '{}' does not exist in '{}'.", row_id, table_name);
            }
        } else {
            println!("Table '{}' does not exist.", table_name);
        }
    }

    /// Delete a row from a table.
    pub fn delete_row(&mut self, table_name: &str, row_id: &str) {
        if let Some(table) = self.tables.get_mut(table_name) {
            if table.delete_row(row_id) {
                println!("Row '{}' deleted from '{}'.", row_id, table_name);
            } else {
                println!("Row '{}' not found in '{}'.", row_id, table_name);
            }
        } else {
            println!("Table '{}' does not exist.", table_name);
        }
    }

    /// Print the contents of a table for debugging.
    pub fn print_table(&self, table_name: &str) {
        if let Some(table) = self.tables.get(table_name) {
            println!("Table '{}':", table_name);
            table.print_table();
        } else {
            println!("Table '{}' does not exist.", table_name);
        }
    }

    /// Save a table to a text file in CSV format, appending if the file already exists.
    /// The first row lists columns in alphabetical order, preceded by "row_id".
    /// TODO: Convert to binary format using a crate like `bincode` if needed.
    pub fn save_table(&self, table_name: &str, file_name: &str) {
        match self.tables.get(table_name) {
            Some(table) => {
                // Collect columns in sorted order for consistent CSV output
                let mut columns_in_order: Vec<_> = table.columns.iter().cloned().collect();
                columns_in_order.sort();
    
                // Always recreate the file instead of appending
                let file_result = File::create(file_name);
    
                match file_result {
                    Ok(file) => {
                        let mut writer = BufWriter::new(file);
    
                        // Write header
                        let header = {
                            let mut hdr = vec!["row_id".to_string()];
                            hdr.extend(columns_in_order.iter().cloned());
                            hdr.join(",")
                        };
                        writeln!(writer, "{}", header).unwrap();
    
                        // Write all rows
                        for (row_id, row_data) in &table.rows {
                            let mut row_vec = vec![row_id.clone()];
                            for col in &columns_in_order {
                                row_vec.push(row_data.get(col).cloned().unwrap_or_default());
                            }
                            writeln!(writer, "{}", row_vec.join(",")).unwrap();
                        }
    
                        println!("Table '{}' saved to '{}'.", table_name, file_name);
                    }
                    Err(e) => {
                        println!("Error creating file '{}': {}", file_name, e);
                    }
                }
            }
            None => {
                println!("Table '{}' does not exist.", table_name);
            }
        }
    }
    
}
