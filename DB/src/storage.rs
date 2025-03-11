use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, BufRead, BufWriter};

struct Memtable {
    data: BTreeMap<String, String>,
}