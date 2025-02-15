# **Rust SimpleDB**  
*A minimal in-memory key-value store with persistence, built from scratch in Rust.*  

## **🚀 Features**
- ✅ **Simple Key-Value Storage** (like a mini Redis)  
- ✅ **Persistent Storage** (`db.txt`)  
- ✅ **Command-Line REPL Interface**  
- ✅ **Basic CRUD Operations (`SET`, `GET`, `DELETE`)**  
- ✅ **Lightweight & Dependency-Free**  

---

## **📌 Getting Started**

### **1️⃣ CD in**
```sh
cd DB
```

### **2️⃣ Build & Run**
```sh
cargo run
```

### **3️⃣ Usage**
Once the program starts, you can enter commands:

```sh
> SET name Alice
OK
> GET name
Alice
> DELETE name
Deleted
> GET name
(nil)
> EXIT
Bye!
```

---

## **🛠 Project Structure**
```
rust-simple-db/
│── src/
│   ├── main.rs   # REPL & CLI interface
│   ├── db.rs     # Database logic (in-memory + persistence)
│── db.txt        # Persistent storage file
│── Cargo.toml    # Rust dependencies
```

---

## **📝 Commands**
| Command          | Description                     | Example              |
|-----------------|---------------------------------|----------------------|
| `SET key value` | Store a key-value pair         | `SET name Alice`     |
| `GET key`       | Retrieve a value by key        | `GET name` → Alice   |
| `DELETE key`    | Remove a key from the database | `DELETE name`        |
| `EXIT`          | Save data & close the program  | `EXIT`               |

---

## **💡 Next Steps**
🔹 Add **Concurrency** using `tokio::sync::RwLock`  
🔹 Implement **Leader-Follower Replication**  
🔹 Improve **Storage Format** (JSON, binary)  

---

## **📜 License**
This project is open-source under the MIT License.  

---

Would you like to add **unit tests** or **leader-follower replication** next? 🚀