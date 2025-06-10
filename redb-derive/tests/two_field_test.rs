use redb::Value;
use redb_derive::Value;

#[derive(Value, Debug)]
struct TwoFieldStruct {
    id: u32,
    name: String,
}

#[test]
fn test_two_field() {
    let original = TwoFieldStruct {
        id: 42,
        name: "test".to_string(),
    };
    let bytes = TwoFieldStruct::as_bytes(&original);
    let deserialized = TwoFieldStruct::from_bytes(&bytes);
    assert_eq!(original.id, deserialized.id);
    assert_eq!(original.name, deserialized.name);
}
