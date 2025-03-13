use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Write, BufWriter};
// use std::path::Path;

/// A Table stores a set of columns and a mapping of row IDs to (column, value) data.
/// - `columns` tracks which columns exist in this table.
/// - `rows` is a BTreeMap so rows are kept in a sorted order by row_id (for predictable iteration).
#[derive(Debug)]
struct Table {
    columns: HashSet<String>,  // List of allowed column names
    rows: BTreeMap<String, HashMap<String, String>>, // row_id -> { column_name -> value }
}

impl Table {
    fn new() -> Self {
        Table {
            columns: HashSet::new(),
            rows: BTreeMap::new(),
        }
    }

    /// Add a new column to the table. Existing rows do not automatically get a value for this column.
    fn add_column(&mut self, column_name: &str) {
        self.columns.insert(column_name.to_string());
    }

    /// Insert or update a row with (column -> value) pairs; restrict columns to those known in `columns`.
    fn insert_row(&mut self, row_id: &str, data: HashMap<String, String>) {
        // Only allow data for columns that exist in this table.
        let valid_data: HashMap<String, String> = data
            .into_iter()
            .filter(|(col, _)| self.columns.contains(col))
            .collect();

        // Upsert (insert if none, update if it exists).
        self.rows
            .entry(row_id.to_string())
            .and_modify(|existing| {
                for (col, val) in &valid_data {
                    existing.insert(col.clone(), val.clone());
                }
            })
            .or_insert(valid_data);
    }

    /// Retrieve data for a specific row.
    fn get_row(&self, row_id: &str) -> Option<&HashMap<String, String>> {
        self.rows.get(row_id)
    }

    /// Delete a specific row by row_id.
    fn delete_row(&mut self, row_id: &str) -> bool {
        self.rows.remove(row_id).is_some()
    }

    /// Print the table contents (for demo).
    fn print_table(&self) {
        println!("Columns: {:?}", self.columns);
        for (row_id, row_data) in &self.rows {
            println!("Row '{}': {:?}", row_id, row_data);
        }
    }
}

/// A Database holds multiple named tables in a HashMap.
#[derive(Debug)]
struct Database {
    tables: HashMap<String, Table>,
    operations_since_save: usize,  // Track how many inserts/updates since last save
    save_threshold: usize,         // Automatically save after this many operations
}
impl Database {
    fn new() -> Self {
        Database {
            tables: HashMap::new(),
            operations_since_save: 0,
            save_threshold: 5, // Example threshold
        }
    }

    /// Create a table if it doesn’t exist.
    fn create_table(&mut self, table_name: &str) {
        if self.tables.contains_key(table_name) {
            println!("Table '{}' already exists.", table_name);
        } else {
            self.tables.insert(table_name.to_string(), Table::new());
            println!("Table '{}' created.", table_name);
        }
    }

    /// Add a column to an existing table.
    fn add_column(&mut self, table_name: &str, column_name: &str) {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.add_column(column_name);
            println!("Added column '{}' to table '{}'.", column_name, table_name);
        } else {
            println!("Table '{}' does not exist.", table_name);
        }
    }

    /// Insert or update data in a table.
    fn insert_row(&mut self, table_name: &str, row_id: &str, data: HashMap<String, String>) {
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
    fn get_row(&self, table_name: &str, row_id: &str) {
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
    fn delete_row(&mut self, table_name: &str, row_id: &str) {
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
    fn print_table(&self, table_name: &str) {
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
    fn save_table(&self, table_name: &str, file_name: &str) {
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

/// Simple REPL to parse commands and interact with the Database.
fn main() {
    let mut db = Database::new();

    println!("Welcome to the RustDB with dynamic columns and multiple tables!");
    println!("Type 'help' for a list of commands.\n");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            println!("Error reading input.");
            continue;
        }

        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "help" => {
                println!("Commands:");
                println!("  CREATE TABLE <tablename>");
                println!("  ADD COLUMN <tablename> <columnname>");
                println!("  INSERT <tablename> <row_id> <col1=value1> <col2=value2> ...");
                println!("  GET <tablename> <row_id>");
                println!("  DELETE <tablename> <row_id>");
                println!("  TABLES (lists all tables)");
                println!("  PRINT <tablename> (prints table contents)");
                println!("  EXIT");
            }

            "create" if parts.len() == 3 && parts[1].to_lowercase() == "table" => {
                db.create_table(parts[2]);
            }

            "add" if parts.len() == 4 && parts[1].to_lowercase() == "column" => {
                db.add_column(parts[2], parts[3]);
            }

            "insert" => {
                // Example: INSERT table row_id col1=val1 col2=val2
                if parts.len() < 4 {
                    println!("Usage: INSERT <tablename> <row_id> <col=value> <col=value> ...");
                    continue;
                }
                let table_name = parts[1];
                let row_id = parts[2];

                let mut data = HashMap::new();
                for kv_pair in &parts[3..] {
                    if let Some(eq_pos) = kv_pair.find('=') {
                        let key = &kv_pair[..eq_pos];
                        let val = &kv_pair[eq_pos + 1..];
                        data.insert(key.to_string(), val.to_string());
                    }
                }
                db.insert_row(table_name, row_id, data);
            }

            "get" if parts.len() == 3 => {
                // Example: GET table row_id
                db.get_row(parts[1], parts[2]);
            }

            "delete" if parts.len() == 3 => {
                // Example: DELETE table row_id
                db.delete_row(parts[1], parts[2]);
            }

            "tables" => {
                println!("Existing tables:");
                for t in db.tables.keys() {
                    println!("  {}", t);
                }
            }

            "print" if parts.len() == 2 => {
                db.print_table(parts[1]);
            }

            "save" => {
                // Usage: SAVE <tablename> <filename>
                if parts.len() != 3 {
                    println!("Usage: SAVE <tablename> <filename>");
                } else {
                    db.save_table(parts[1], parts[2]);
                }
            }

            "exit" => {
                println!("Exiting RustDB.");
                break;
            }

            _ => {
                println!("Unknown command. Type 'help' for a list of commands.");
            }
        }
    }
}