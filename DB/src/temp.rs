use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

#[derive(Debug)]
struct Table {
    rows: BTreeMap<String, HashMap<String, String>>, // row_id -> { column_name -> value }
}

#[derive(Debug)]
struct LSMDatabase {
    tables: HashMap<String, Table>, // table_name -> Table
    sstable_dir: String,
    sstable_count: usize,
}

impl LSMDatabase {
    fn new(sstable_dir: &str) -> Self {
        fs::create_dir_all(sstable_dir).unwrap(); // Ensure directory exists
        LSMDatabase {
            tables: HashMap::new(),
            sstable_dir: sstable_dir.to_string(),
            sstable_count: 0,
        }
    }

    // 🔹 Create a new table
    fn create_table(&mut self, table_name: &str) {
        self.tables.insert(table_name.to_string(), Table { rows: BTreeMap::new() });
    }

    // 🔹 Insert row into a table
    fn insert(&mut self, table_name: &str, row_id: &str, columns: HashMap<String, String>) {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.rows.insert(row_id.to_string(), columns);
        } else {
            println!("Table '{}' not found!", table_name);
        }

        // Flush to SSTable when too many rows
        if self.tables.len() > 2 {
            self.flush_to_sstable();
        }
    }

    // 🔹 Retrieve row from a table
    fn get_row(&self, table_name: &str, row_id: &str) -> Option<&HashMap<String, String>> {
        self.tables.get(table_name)?.rows.get(row_id)
    }

    // 🔹 Retrieve specific column from a row
    fn get_column(&self, table_name: &str, row_id: &str, column_name: &str) -> Option<String> {
        self.tables
            .get(table_name)?
            .rows
            .get(row_id)?
            .get(column_name)
            .cloned()
    }

    // 🔹 Flush in-memory tables to SSTables
    fn flush_to_sstable(&mut self) {
        let sstable_file = format!("{}/sstable_{}.csv", self.sstable_dir, self.sstable_count);
        let mut file = File::create(&sstable_file).unwrap();

        for (table_name, table) in &self.tables {
            writeln!(file, "[TABLE:{}]", table_name).unwrap();
            for (row_id, columns) in &table.rows {
                let row_data: Vec<String> = columns.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                writeln!(file, "{}:{}", row_id, row_data.join(",")).unwrap();
            }
        }

        self.tables.clear(); // Clear memory
        self.sstable_count += 1;
        println!("Flushed MemTable to {}", sstable_file);
    }

    // 🔹 Search SSTables for a row
    fn search_sstables(&self, table_name: &str, row_id: &str) -> Option<HashMap<String, String>> {
        for i in (0..self.sstable_count).rev() {
            let filename = format!("{}/sstable_{}.csv", self.sstable_dir, i);
            if let Ok(file) = File::open(&filename) {
                let reader = BufReader::new(file);
                let mut current_table = String::new();

                for line in reader.lines() {
                    let line = line.unwrap();
                    if line.starts_with("[TABLE:") {
                        current_table = line.replace("[TABLE:", "").replace("]", "").to_string();
                    } else if current_table == table_name {
                        let parts: Vec<&str> = line.split(':').collect();
                        if parts.len() == 2 && parts[0] == row_id {
                            let mut row = HashMap::new();
                            for col in parts[1].split(',') {
                                let kv: Vec<&str> = col.split('=').collect();
                                if kv.len() == 2 {
                                    row.insert(kv[0].to_string(), kv[1].to_string());
                                }
                            }
                            return Some(row);
                        }
                    }
                }
            }
        }
        None
    }
}
