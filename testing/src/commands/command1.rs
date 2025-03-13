pub struct command1{
    pub name: String,
    pub description: String,
    pub usage: String,
}

impl command1{
    pub fn new() -> command1{
        command1{
            name: "command1".to_string(),
            description: "This is command1".to_string(),
            usage: "command1".to_string(),
        }
    }
    pub fn printing(&self){
        println!("Name: {}", self.name);
        println!("Description: {}", self.description);
        println!("Usage: {}", self.usage);
    }
}