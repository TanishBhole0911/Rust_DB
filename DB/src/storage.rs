use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, BufRead, BufWriter};

/// **Memtable (In-Memory Storage)**
struct Memtable {
    data: BTreeMap<String, String>,
}

impl Memtable {
    fn new() -> Self {
        Self { data: BTreeMap::new() }
    }

    fn insert(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

/// **Write-Ahead Log (WAL)**
struct WAL {
    file: File,
}

impl WAL {
    fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        Self { file }
    }

    fn log(&mut self, key: &str, value: &str) {
        writeln!(self.file, "{}:{}", key, value).unwrap();
    }

    fn read_logs(path: &str) -> Vec<(String, String)> {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        reader.lines()
            .filter_map(|line| line.ok())
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(2, ':').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].to_string()))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// **SSTables (On-Disk Storage)**
fn flush_to_sstable(memtable: &Memtable, path: &str) {
    let mut file = File::create(path).unwrap();
    for (key, value) in &memtable.data {
        writeln!(file, "{}:{}", key, value).unwrap();
    }
}

fn read_sstable(path: &str, key: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line.unwrap();
        let mut parts = line.splitn(2, ':');
        if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
            if k == key {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// **Compaction (Merge SSTables)**
fn compact_sstables(sstable_paths: Vec<&str>, output_path: &str) {
    let mut merged_data = BTreeMap::new();

    for path in sstable_paths {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.unwrap();
            let mut parts = line.splitn(2, ':');
            if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                merged_data.insert(k.to_string(), v.to_string());
            }
        }
    }

    let mut output_file = BufWriter::new(File::create(output_path).unwrap());
    for (key, value) in merged_data {
        writeln!(output_file, "{}:{}", key, value).unwrap();
    }

    // Remove old SSTables
    for path in sstable_paths {
        std::fs::remove_file(path).unwrap();
    }
}

/// **LSM Tree (Main Database)**
struct LSMTree {
    memtable: Memtable,
    wal: WAL,
    sstable_path: String,
    threshold: usize,
}

impl LSMTree {
    fn new(wal_path: &str, sstable_path: &str, threshold: usize) -> Self {
        let wal = WAL::new(wal_path);
        let memtable = Memtable::new();
        Self { memtable, wal, sstable_path: sstable_path.to_string(), threshold }
    }

    fn insert(&mut self, key: String, value: String) {
        self.wal.log(&key, &value);
        self.memtable.insert(key, value);
        
        if self.memtable.size() >= self.threshold {
            flush_to_sstable(&self.memtable, &self.sstable_path);
            self.memtable = Memtable::new(); // Clear memtable after flush
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }
        read_sstable(&self.sstable_path, key)
    }
}

/// **Test the LSM Tree**
fn main() {
    let mut lsm = LSMTree::new("wal.log", "sstable.txt", 5);

    // Insert some data
    lsm.insert("key1".to_string(), "value1".to_string());
    lsm.insert("key2".to_string(), "value2".to_string());
    lsm.insert("key3".to_string(), "value3".to_string());

    // Retrieve values
    println!("{:?}", lsm.get("key1")); // Some("value1")
    println!("{:?}", lsm.get("key2")); // Some("value2")

    // Insert more to trigger SSTable flush
    lsm.insert("key4".to_string(), "value4".to_string());
    lsm.insert("key5".to_string(), "value5".to_string());
    lsm.insert("key6".to_string(), "value6".to_string());

    // After flush, data should still be accessible
    println!("{:?}", lsm.get("key3")); // Some("value3")

    // Compaction Example
    compact_sstables(vec!["sstable.txt"], "sstable_merged.txt");
    println!("Compaction done!");
}
