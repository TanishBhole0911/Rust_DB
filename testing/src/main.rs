#[warn(unused_imports)]
use std::fs;
use std::path::Path;
mod commands;



fn main() {
    let folder_path = "./src/commands"; // Change this to your folder path
    let mut com = vec![];

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
                            com.push()
                        }
                    }
                    Err(e) => eprintln!("Error reading entry: {}", e),
                }
            }
        }
        Err(e) => eprintln!("Error reading directory: {}", e),
    }
    println!("{:?}", com);
    
}
