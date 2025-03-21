use crate::table;
use crate::table::table::Table;


use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::vec;



pub enum Result<T, E> {
    Ok(T),
    Err(E),
 }

pub struct Database {
    pub tables: HashMap<String, Table>,
    pub operations_since_save: usize,  // Track how many inserts/updates since last save
    pub save_threshold: usize,
    pub res_message: String,
}

impl Database{
    pub fn new()-> Self{
        Database{
            tables: HashMap::new(),
            operations_since_save: 0,
            save_threshold: 5,
            res_message: "".to_string(),
        }
    }
    
    //Check if table exists
    pub fn check_table(&self, table_name: &str) -> bool {
        if self.tables.contains_key(table_name) {
            return true;
        }else {
            return false;
        }
    }

    //Create a table if it doesn't exist
    // required -> table_name
    // return -> table_name
    pub fn create_table(&mut self, table_name: &str) -> Result<String, &str> {
        if self.tables.contains_key(table_name) {
            println!("Table '{}' already exists.", table_name);
            self.res_message = format!("Table '{}' already exists.", table_name);
            return Result::Err(self.res_message.as_str());
        } else {
            self.tables.insert(table_name.to_string(), Table::new());
            println!("Table '{}' created.", table_name);
            return Result::Ok(table_name.to_string());
        }
    }

    //Add a column to an existing table
    // required -> table_name, column_name
    // return -> vec![column_name, table_name]
    pub fn add_column(&mut self, table_name: &str, column_name: &str) -> Result<Vec<String>, &str> {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.add_column(column_name);
            println!("Added column '{}' to table '{}'.", column_name, table_name);
            self.res_message = format!("Added column '{}' to table '{}'.", column_name, table_name);
            return Result::Ok(vec![column_name.to_string(), table_name.to_string()]);
        } else {
            println!("Table '{}' does not exist.", table_name);
            return Result::Err("Table does not exist");
        }
    }


    // Get rows from a table
    // required -> table_name, row_id
    // return -> vec![row_id, row_data]
    pub fn get_row(&mut self, table_name: &str, row_id: &str) -> Result<Vec<String>, &str> {
        if let Some(table) = self.tables.get(table_name) {
            if let Some(row) = table.get_row(row_id) {
                println!("Row '{}': {:?}", row_id, row);
                let row_string = format!("{:?}", row);
                return Result::Ok(vec![row_id.to_string(), row_string]);
            } else {
                println!("Row '{}' does not exist in '{}'.", row_id, table_name);
                self.res_message = format!("Row '{}' does not exist in '{}'.", row_id, table_name);
                return Result::Err("Row does not exist");
            }
        } else {
            println!("Table '{}' does not exist.", table_name);
            self.res_message = format!("Table '{}' does not exist.", table_name);
            return Result::Err("Table does not exist");
        }
    }

    /// Delete a row from a table.
    /// required -> table_name, row_id
    /// return -> vec![row_id, table_name]
    pub fn delete_row(&mut self, table_name: &str, row_id: &str) -> Result<Vec<String>, &str> {
        if let Some(table) = self.tables.get_mut(table_name) {
            if table.delete_row(row_id) {
                println!("Row '{}' deleted from '{}'.", row_id, table_name);
                return Result::Ok(vec![row_id.to_string(), table_name.to_string()]);
            } else {
                println!("Row '{}' not found in '{}'.", row_id, table_name);
                return Result::Err("Row not found");
            }
        } else {
            println!("Table '{}' does not exist.", table_name);
            return Result::Err("Table does not exist");
        }
    }
    

    // get table
    // required -> table_name
    // return -> table
    pub fn get_table(&self, table_name: &str) -> Result<&Table, &str> {
        if let Some(table) = self.tables.get(table_name) {
            return Result::Ok(table);
        } else {
            return Result::Err("Table does not exist");
        }
    }


    //save table
    // required -> table_name, file_name
    // return -> vec![table_name, file_name]
    pub fn save_table(&self, table_name: &str, file_name: &str) -> Result<Vec<String>, &str> {
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
                        return Result::Ok(vec![table_name.to_string(), file_name.to_string()]);
                    }
                    Err(e) => {
                        println!("Error creating file '{}': {}", file_name, e);
                        return Result::Err(Box::leak(e.to_string().into_boxed_str()));
                    }
                }
            }
            None => {
                println!("Table '{}' does not exist.", table_name);

                return Result::Err("Table does not exist");
            }
        }
    }    

}