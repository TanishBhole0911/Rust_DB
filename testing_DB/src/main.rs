use std::collections::HashMap;
use std::io::{self, Write};

mod db;
use db::Database;

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