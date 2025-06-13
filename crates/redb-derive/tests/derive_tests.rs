use redb::{Database, TableDefinition, Value};
use redb_derive::Value;
use std::fmt::Debug;
use tempfile::NamedTempFile;

#[derive(Value, Debug, PartialEq)]
struct SimpleStruct {
    id: u32,
    name: String,
}

#[derive(Value, Debug, PartialEq)]
struct TupleStruct(u64, bool);

#[derive(Value, Debug, PartialEq)]
struct SingleField {
    value: i32,
}

#[derive(Value, Debug, PartialEq)]
struct ComplexStruct<'inner, 'inner2> {
    tuple_field: (u8, u16, u32),
    array_field: [(u8, Option<u16>); 2],
    reference: &'inner str,
    reference2: &'inner2 str,
}

fn test_helper<V: Value + 'static>(value: <V as Value>::SelfType<'_>, expected_type_name: &str)
where
    for<'x> <V as Value>::SelfType<'x>: PartialEq,
{
    let type_name = V::type_name();
    assert_eq!(type_name.name(), expected_type_name);

    let file = NamedTempFile::new().unwrap();
    let db = Database::create(file.path()).unwrap();
    let table_def: TableDefinition<u32, V> = TableDefinition::new("test");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(1, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    // Due to the lifetimes of SelfType we can't compare the values themselves, so instead compare
    // the serialized representation
    let retrieved_value = retrieved.value();
    let expected_bytes = V::as_bytes(&value);
    let bytes = V::as_bytes(&retrieved_value);
    assert_eq!(expected_bytes.as_ref(), bytes.as_ref());
}

#[test]
fn test_simple_struct() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };
    test_helper::<SimpleStruct>(original, "SimpleStruct {id: u32, name: String}");
}

#[test]
fn test_tuple_struct() {
    let original = TupleStruct(123456789, true);
    test_helper::<TupleStruct>(original, "TupleStruct(u64, bool)");
}

#[test]
fn test_single_field() {
    let original = SingleField { value: -42 };
    test_helper::<SingleField>(original, "SingleField {value: i32}");
}

#[test]
fn test_complex_struct() {
    let original = ComplexStruct {
        tuple_field: (1, 2, 3),
        array_field: [(4, Some(5)), (6, None)],
        reference: "hello",
        reference2: "world",
    };

    let expected_name = "ComplexStruct {tuple_field: (u8,u16,u32), array_field: [(u8,Option<u16>);2], reference: &str, reference2: &str}";
    test_helper::<ComplexStruct>(original, expected_name);
}
