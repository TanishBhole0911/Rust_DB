use std::collections::{BTreeMap, HashMap, HashSet};

pub struct Table {
    pub columns: HashSet<String>,  // List of allowed column names
    pub rows: BTreeMap<String, HashMap<String, String>>, // row_id -> { column_name -> value }
}

impl Table {
    pub fn new() -> Self {
        Table {
            columns: HashSet::new(),
            rows: BTreeMap::new(),
        }
    }

    /// Add a new column to the table. Existing rows do not automatically get a value for this column.
    pub fn add_column(&mut self, column_name: &str) {
        self.columns.insert(column_name.to_string());
    }

    /// Insert or update a row with (column -> value) pairs; restrict columns to those known in `columns`.
    pub fn insert_row(&mut self, row_id: &str, data: HashMap<String, String>) {
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
    pub fn get_row(&self, row_id: &str) -> Option<&HashMap<String, String>> {
        self.rows.get(row_id)
    }
    /// Delete a specific row by row_id.
    pub fn delete_row(&mut self, row_id: &str) -> bool {
        self.rows.remove(row_id).is_some()
    }

    /// Print the table contents (for demo).
    pub fn print_table(&self) {
        println!("Columns: {:?}", self.columns);
        for (row_id, row_data) in &self.rows {
            println!("Row '{}': {:?}", row_id, row_data);
        }
    }
}