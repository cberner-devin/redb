use redb::Value;
use redb_derive::Value;

#[derive(Value, Debug)]
struct UnitStruct;

#[test]
fn test_unit_struct() {
    let original = UnitStruct;
    let bytes = UnitStruct::as_bytes(&original);
    let _deserialized = UnitStruct::from_bytes(&bytes);
}
