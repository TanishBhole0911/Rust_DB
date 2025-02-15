mod db;
use db::Database;
use std::io::{self, Write};

fn main() {
    let mut db = Database::new("./db.txt").expect("Failed to load database");

    println!("Welcome to RustDB!");
    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let command: Vec<&str> = input.trim().split_whitespace().collect();

        if command.is_empty() {
            continue;
        }

        match command[0].to_uppercase().as_str() {
            "SET" if command.len() == 3 => {
                db.set(command[1], command[2]);
                println!("OK");
            }
            "GET" if command.len() == 2 => {
                match db.get(command[1]) {
                    Some(value) => println!("{}", value),
                    None => println!("(nil)"),
                }
            }
            "DELETE" if command.len() == 2 => {
                if db.delete(command[1]) {
                    println!("Deleted");
                } else {
                    println!("Key not found");
                }
            }
            "EXIT" => {
                db.save().expect("Failed to save database");
                println!("Bye!");
                break;
            }
            _ => println!("Unknown command"),
        }
    }
}
