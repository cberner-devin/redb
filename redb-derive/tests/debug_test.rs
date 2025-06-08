use redb::Value;
use redb_derive::Value;

#[derive(Value, Debug)]
struct DebugStruct {
    id: u32,
}

fn main() {
    println!("Generated code compiles");
}
