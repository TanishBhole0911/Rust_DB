use crate::table::table::Table;
use super::db::Database;
pub enum Result<T, E> {
    Ok(T),
    Err(E),
 }

pub struct Save_table{
    pub name: String,
    pub description: String,
    pub usage: String,
    pub res_message: Result<String, String>,
}

impl Save_table{
    pub fn new() -> Save_table{
        Save_table{
            name: "Save_table".to_string(),
            description: "This is command1".to_string(),
            usage: "Save_table".to_string(),
            res_message: Result::Ok("".to_string()),
        }
    }
    pub fn printing(&self){
        println!("Name: {}", self.name);
        println!("Description: {}", self.description);
        println!("Usage: {}", self.usage);
    }

    pub fn save_table(&mut self, t_name: &str, file_name: &str, db: &mut Database) -> () {
        self.res_message = match db.save_table(t_name, file_name) {
            super::db::Result::Ok(val) => {
                println!("Table '{}' saved to '{}'.", t_name, file_name);
                Result::Ok(val.join(", "))
            },
            super::db::Result::Err(err) => Result::Err(err.to_string()),
        };
    }
}