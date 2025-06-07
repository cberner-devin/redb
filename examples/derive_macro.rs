#[cfg(feature = "derive")]
use redb::{Database, TableDefinition, Value};

#[cfg(feature = "derive")]
use redb_derive::Value as DeriveValue;

#[cfg(feature = "derive")]
use tempfile::NamedTempFile;

#[cfg(feature = "derive")]
#[derive(Debug, DeriveValue)]
#[redb(type_name = "User")]
struct User {
    id: u64,
    username: String,
    email: String,
    active: bool,
}

#[cfg(feature = "derive")]
#[derive(Debug, DeriveValue)]
#[redb(type_name = "Point")]
struct Point(f64, f64);

#[cfg(feature = "derive")]
#[derive(Debug, DeriveValue)]
#[redb(type_name = "Config")]
struct Config {
    max_connections: u32,
    timeout_seconds: Option<u32>,
    server_name: String,
}

#[cfg(feature = "derive")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmpfile = NamedTempFile::new()?;
    let db = Database::create(tmpfile.path())?;

    let users_table: TableDefinition<u64, User> = TableDefinition::new("users");
    let points_table: TableDefinition<String, Point> = TableDefinition::new("points");
    let config_table: TableDefinition<&str, Config> = TableDefinition::new("config");

    let write_txn = db.begin_write()?;
    {
        let mut users = write_txn.open_table(users_table)?;
        let mut points = write_txn.open_table(points_table)?;
        let mut config = write_txn.open_table(config_table)?;

        let user = User {
            id: 1,
            username: "alice".to_string(),
            email: "alice@example.com".to_string(),
            active: true,
        };
        users.insert(&user.id, &user)?;

        let point = Point(3.14, 2.71);
        points.insert(&"origin".to_string(), &point)?;

        let config_data = Config {
            max_connections: 100,
            timeout_seconds: Some(30),
            server_name: "my-server".to_string(),
        };
        config.insert("main", &config_data)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    {
        let users = read_txn.open_table(users_table)?;
        let points = read_txn.open_table(points_table)?;
        let config = read_txn.open_table(config_table)?;

        if let Some(user) = users.get(&1)? {
            println!("User: {:?}", user.value());
            println!("User type name: {:?}", User::type_name());
        }

        if let Some(point) = points.get(&"origin".to_string())? {
            println!("Point: {:?}", point.value());
            println!("Point type name: {:?}", Point::type_name());
        }

        if let Some(config) = config.get("main")? {
            println!("Config: {:?}", config.value());
            println!("Config type name: {:?}", Config::type_name());
        }
    }

    println!("\nFixed width information:");
    println!("User fixed width: {:?}", User::fixed_width());
    println!("Point fixed width: {:?}", Point::fixed_width());
    println!("Config fixed width: {:?}", Config::fixed_width());

    Ok(())
}

#[cfg(not(feature = "derive"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("This example requires the 'derive' feature to be enabled.");
    println!("Run with: cargo run --example derive_macro --features derive");
    Ok(())
}
