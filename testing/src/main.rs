#[warn(unused_imports)]
use std::fs;

pub mod table;


// use std::path::Path;
mod commands;
const FOLDER_PATH: &str = "./src/commands";
use commands::{command1, command2, db};


fn get_command_names()-> Vec<String> {
    let folder_path = FOLDER_PATH;
    let mut files = vec![];
    let mut file_name = String::new();

    // Read the contents of the directory
    match fs::read_dir(folder_path) {
        Ok(entries) => {
            // Iterate over the entries
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.extension() == Some(std::ffi::OsStr::new("rs")) && path.file_name() != Some(std::ffi::OsStr::new("mod.rs")) {
                            println!("{:?}", path.file_name().unwrap());
                            file_name = path.file_name().unwrap().to_str().unwrap().to_string();
                            files.push(file_name.split(".").next().unwrap().to_string());
                        }
                    }
                    Err(e) => eprintln!("Error reading entry: {}", e),
                }
            }
        }
        Err(e) => eprintln!("Error reading directory: {}", e),
    }
    println!("{:?}", files);
    return files;
}



fn main() {
    let mut db = db::Database::new();


    let x = get_command_names();
    print!("{:?}", x);
    let mut Create_Table = command1::Create_Table::new();
    // Create_Table.printing();
    Create_Table.create_table("table1", &mut db);
    Create_Table.create_table_with_columns("table2", vec!["column1", "column2"], &mut db);
    let mut Save_table = command2::Save_table::new();
    // Save_table.printing();
    Save_table.save_table("table2", "table1.csv", &mut db);
    Create_Table.save_table("table1", "table11.csv", &mut db);
}

