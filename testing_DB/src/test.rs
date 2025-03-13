use std::collections::HashMap;
use std::fs::{remove_file, read_to_string};

use rust_db::Database; // Adjust the crate name/path to match your project setup

#[test]
fn test_end_to_end() {
    // Clean up any old file from a previous run
    let _ = remove_file("employees.csv");

    let mut db = Database::new();

    // 1. Create table
    db.create_table("employees");

    // 2. Add columns
    db.add_column("employees", "name");
    db.add_column("employees", "position");

    // 3. Insert row
    let mut data = HashMap::new();
    data.insert("name".to_string(), "Alice".to_string());
    data.insert("position".to_string(), "Engineer".to_string());
    db.insert_row("employees", "1001", data);

    // 4. Retrieve row
    db.get_row("employees", "1001");

    // 5. Print
    db.print_table("employees");

    // 6. Save
    db.save_table("employees", "employees.csv");

    // Verify the file was created and contains expected data
    let csv_contents = read_to_string("employees.csv").expect("Could not read CSV file");
    assert!(csv_contents.contains("row_id,name,position"));
    assert!(csv_contents.contains("1001,Alice,Engineer"));
}