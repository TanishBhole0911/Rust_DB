use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};

#[derive(Debug)]
pub struct Database {
    storage: HashMap<String, String>,
    file_path: String,
}

impl Database {
    // Initialize DB and load data from file
    pub fn new(file_path: &str) -> Result<Self, std::io::Error> {
        let mut storage = HashMap::new();
        let file = OpenOptions::new()
            .read(true)
            .write(true)  // Ensure we can write to the file
            .create(true) // Create if it doesn't exist
            .open(file_path)?;

        let reader = BufReader::new(file);

        for line in reader.lines().filter_map(Result::ok) {
            let parts: Vec<&str> = line.splitn(2, ',').collect();
            if parts.len() == 2 {
                storage.insert(parts[0].to_string(), parts[1].to_string());
            }
        }

        Ok(Self {
            storage,
            file_path: file_path.to_string(),
        })
    }

    // Insert or update a key-value pair
    pub fn set(&mut self, key: &str, value: &str) {
        self.storage.insert(key.to_string(), value.to_string());
    }

    // Retrieve a value by key
    pub fn get(&self, key: &str) -> Option<String> {
        self.storage.get(key).cloned()
    }

    // Delete a key-value pair
    pub fn delete(&mut self, key: &str) -> bool {
        self.storage.remove(key).is_some()
    }

    // Save database to disk
    pub fn save(&self) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new().write(true).truncate(true).open(&self.file_path)?;
        for (key, value) in &self.storage {
            writeln!(file, "{},{}", key, value)?;
        }
        Ok(())
    }
}
