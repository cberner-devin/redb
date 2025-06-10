use redb::Value;
use redb_derive::Value;

#[derive(Value, Debug)]
struct SimpleTest {
    id: u32,
}

#[test]
fn test_simple() {
    let _type_name = SimpleTest::type_name();
}
