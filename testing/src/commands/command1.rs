use crate::table::table::Table;
use super::db::Database;
pub enum Result<T, E> {
    Ok(T),
    Err(E),
 }

pub struct Create_Table{
    pub name: String,
    pub description: String,
    pub usage: String,
    pub res_message: Result<String, String>,
}

impl Create_Table{
    pub fn new() -> Create_Table{
        Create_Table{
            name: "Create_Table".to_string(),
            description: "This is Create_Table".to_string(),
            usage: "Create_Table".to_string(),
            res_message: Result::Ok("".to_string()),
        }
    }
    pub fn printing(&self){
        println!("Name: {}", self.name);
        println!("Description: {}", self.description);
        println!("Usage: {}", self.usage);
    }

    pub fn create_table(&mut self, t_name: &str, db: &mut Database) -> () {
        self.res_message = match db.create_table(t_name) {
            super::db::Result::Ok(val) => Result::Ok(val),
            super::db::Result::Err(err) => Result::Err(err.to_string()),
        };
    }

    
    pub fn create_table_with_columns(&mut self, t_name: &str, columns: Vec<&str>, db: &mut Database) -> () {

        self.res_message = match db.create_table(t_name) {
            super::db::Result::Ok(val) => {
                for column in columns {
                    match db.add_column(t_name, column) {
                        super::db::Result::Ok(_) => (),
                        super::db::Result::Err(err) => {
                            self.res_message = Result::Err(err.to_string());
                            return;
                        }
                    }
                }
                Result::Ok(val)
            },
            super::db::Result::Err(err) => Result::Err(err.to_string()),
        };
    }
}