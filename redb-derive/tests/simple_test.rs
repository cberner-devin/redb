use redb::Value;
use redb_derive::Value;

#[derive(Value, Debug)]
struct SimpleStruct {
    id: u32,
    name: String,
}

#[test]
fn test_simple_struct_compiles() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };

    let bytes = SimpleStruct::as_bytes(&original);
    let deserialized = SimpleStruct::from_bytes(&bytes);

    assert_eq!(original.id, deserialized.id);
    assert_eq!(original.name, deserialized.name);
}

#[test]
fn test_type_name() {
    let expected = "SimpleStruct {id: u32, name: String}";
    let type_name = SimpleStruct::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);
}
