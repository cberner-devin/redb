# redb-derive

Procedural macros for the [redb](https://crates.io/crates/redb) embedded database.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
redb = { version = "2.6.0", features = ["derive"] }
```

Then you can derive the `Value` trait for your structs:

```rust
use redb::{Database, TableDefinition};
use redb_derive::Value;

#[derive(Debug, Value)]
#[redb(type_name = "User")]
struct User {
    id: u64,
    name: String,
    email: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("example.db")?;
    let table: TableDefinition<u64, User> = TableDefinition::new("users");
    
    let write_txn = db.begin_write()?;
    {
        let mut users = write_txn.open_table(table)?;
        let user = User {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        };
        users.insert(&user.id, &user)?;
    }
    write_txn.commit()?;
    
    let read_txn = db.begin_read()?;
    let users = read_txn.open_table(table)?;
    if let Some(user) = users.get(&1)? {
        println!("Found user: {:?}", user.value());
    }
    
    Ok(())
}
```

## Features

- **Type Safety**: The derive macro generates type-safe serialization code
- **Custom Type Names**: Use the `#[redb(type_name = "...")]` attribute to specify a custom type name
- **Field Type Tracking**: The generated `TypeName` includes all field types to detect schema changes
- **Tuple-based Serialization**: Uses redb's efficient tuple serialization under the hood
- **Mixed Field Types**: Supports both fixed-width and variable-width fields

## Supported Struct Types

### Named Fields
```rust
#[derive(Value)]
#[redb(type_name = "Person")]
struct Person {
    id: u32,
    name: String,
    age: u8,
}
```

### Tuple Structs
```rust
#[derive(Value)]
#[redb(type_name = "Point")]
struct Point(f64, f64);
```

### Single Fields
```rust
#[derive(Value)]
#[redb(type_name = "UserId")]
struct UserId {
    value: u64,
}
```

## Type Name Generation

The derive macro generates a `TypeName` that combines your custom type name with the field types:

```rust
#[derive(Value)]
#[redb(type_name = "User")]
struct User {
    id: u32,
    name: String,
}

// Generated TypeName: "User(u32,String)"
```

This ensures that if you change the field types, the database will detect the schema change and prevent data corruption.

## Requirements

- All field types must implement the `Value` trait
- The `#[redb(type_name = "...")]` attribute is required
- Unit structs and empty structs are not supported

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
