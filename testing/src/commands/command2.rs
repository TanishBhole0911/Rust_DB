pub struct command2{
    pub name: String,
    pub description: String,
    pub usage: String,
}

impl command2{
    pub fn new() -> command2{
        command2{
            name: "command2".to_string(),
            description: "This is command1".to_string(),
            usage: "command2".to_string(),
        }
    }
    pub fn printing(&self){
        println!("Name: {}", self.name);
        println!("Description: {}", self.description);
        println!("Usage: {}", self.usage);
    }
}