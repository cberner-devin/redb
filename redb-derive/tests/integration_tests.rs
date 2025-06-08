use redb::{Database, TableDefinition, Value};
use redb_derive::Value;
use tempfile::NamedTempFile;

#[derive(Value, Debug)]
struct SimpleStruct {
    id: u32,
    name: String,
}

#[derive(Value, Debug)]
struct TupleStruct(u64, bool);

#[derive(Value, Debug)]
struct SingleField {
    value: i32,
}

#[derive(Value, Debug)]
struct ComplexStruct<'inner, 'inner2> {
    tuple_field: (u8, u16, u32),
    array_field: [(u8, Option<u16>); 2],
    reference: &'inner str,
    reference2: &'inner2 [u8],
}

const TABLE: TableDefinition<u32, SimpleStruct> = TableDefinition::new("test_table");
const TUPLE_TABLE: TableDefinition<u32, TupleStruct> = TableDefinition::new("tuple_table");
const SINGLE_TABLE: TableDefinition<u32, SingleField> = TableDefinition::new("single_table");

#[test]
fn test_simple_struct_type_name() {
    let expected = "SimpleStruct {id: u32, name: String}";
    let type_name = SimpleStruct::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);
}

#[test]
fn test_tuple_struct_type_name() {
    let expected = "TupleStruct(u64, bool)";
    let type_name = TupleStruct::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);
}

#[test]
fn test_single_field_type_name() {
    let expected = "SingleField {value: i32}";
    let type_name = SingleField::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);
}

#[test]
fn test_complex_struct_type_name() {
    let expected = "ComplexStruct {tuple_field: (u8, u16, u32), array_field: [(u8, Option<u16>); 2], reference: &str, reference2: &[u8]}";
    let type_name = ComplexStruct::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);
}

#[test]
fn test_simple_struct_round_trip() {
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
fn test_tuple_struct_round_trip() {
    let original = TupleStruct(123456789, true);

    let bytes = TupleStruct::as_bytes(&original);
    let deserialized = TupleStruct::from_bytes(&bytes);

    assert_eq!(original.0, deserialized.0);
    assert_eq!(original.1, deserialized.1);
}

#[test]
fn test_single_field_round_trip() {
    let original = SingleField { value: -42 };

    let bytes = SingleField::as_bytes(&original);
    let deserialized = SingleField::from_bytes(&bytes);

    assert_eq!(original.value, deserialized.value);
}

#[test]
fn test_complex_struct_round_trip() {
    let data = b"test data";
    let original = ComplexStruct {
        tuple_field: (1, 2, 3),
        array_field: [(4, Some(5)), (6, None)],
        reference: "hello",
        reference2: data,
    };

    let bytes = ComplexStruct::as_bytes(&original);
    let deserialized = ComplexStruct::from_bytes(&bytes);

    assert_eq!(original.tuple_field, deserialized.tuple_field);
    assert_eq!(original.array_field, deserialized.array_field);
    assert_eq!(original.reference, deserialized.reference);
    assert_eq!(original.reference2, deserialized.reference2);
}

#[test]
fn test_simple_struct_database_storage() {
    let tmpfile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        let value = SimpleStruct {
            id: 42,
            name: "database test".to_string(),
        };
        table.insert(1, value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap().value();
    
    assert_eq!(retrieved.id, 42);
    assert_eq!(retrieved.name, "database test");
}

#[test]
fn test_tuple_struct_database_storage() {
    let tmpfile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TUPLE_TABLE).unwrap();
        let value = TupleStruct(987654321, false);
        table.insert(1, value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TUPLE_TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap().value();
    
    assert_eq!(retrieved.0, 987654321);
    assert_eq!(retrieved.1, false);
}

#[test]
fn test_single_field_database_storage() {
    let tmpfile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SINGLE_TABLE).unwrap();
        let value = SingleField { value: -999 };
        table.insert(1, value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SINGLE_TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap().value();
    
    assert_eq!(retrieved.value, -999);
}
