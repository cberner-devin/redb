use redb::Value;
use redb_derive::Value;

#[derive(Value, Debug)]
struct SingleFieldStruct {
    id: u32,
}

#[test]
fn test_single_field() {
    let original = SingleFieldStruct { id: 42 };
    let bytes = SingleFieldStruct::as_bytes(&original);
    let deserialized = SingleFieldStruct::from_bytes(&bytes);
    assert_eq!(original.id, deserialized.id);
}
