use redb::{Database, TableDefinition, Value};
use redb_derive::Value;
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

// #[derive(Value, Debug, PartialEq)]
// struct ComplexStruct<'a> {
//     tuple_field: (u8, u16, u32),
//     array_field: [(u8, Option<u16>); 2],
//     reference: &'a str,
// }

#[test]
fn test_simple_struct() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };

    let bytes = SimpleStruct::as_bytes(&original);
    let deserialized = SimpleStruct::from_bytes(&bytes);
    assert_eq!(original, deserialized);

    let type_name = SimpleStruct::type_name();
    let expected_name = "SimpleStruct {id: u32, name: String}";
    assert_eq!(type_name.to_string(), expected_name);

    let file = NamedTempFile::new().unwrap();
    let db = Database::create(file.path()).unwrap();
    const TABLE: TableDefinition<u32, SimpleStruct> = TableDefinition::new("test");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert(1, &original).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    assert_eq!(retrieved.value(), original);
}

#[test]
fn test_tuple_struct() {
    let original = TupleStruct(123456789, true);

    let bytes = TupleStruct::as_bytes(&original);
    let deserialized = TupleStruct::from_bytes(&bytes);
    assert_eq!(original, deserialized);

    let type_name = TupleStruct::type_name();
    let expected_name = "TupleStruct(u64, bool)";
    assert_eq!(type_name.to_string(), expected_name);

    let file = NamedTempFile::new().unwrap();
    let db = Database::create(file.path()).unwrap();
    const TABLE: TableDefinition<u32, TupleStruct> = TableDefinition::new("test");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert(1, &original).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    assert_eq!(retrieved.value(), original);
}

#[test]
fn test_single_field() {
    let original = SingleField { value: -42 };

    let bytes = SingleField::as_bytes(&original);
    let deserialized = SingleField::from_bytes(&bytes);
    assert_eq!(original, deserialized);

    let type_name = SingleField::type_name();
    let expected_name = "SingleField {value: i32}";
    assert_eq!(type_name.to_string(), expected_name);

    let file = NamedTempFile::new().unwrap();
    let db = Database::create(file.path()).unwrap();
    const TABLE: TableDefinition<u32, SingleField> = TableDefinition::new("test");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert(1, &original).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    assert_eq!(retrieved.value(), original);
}

// #[test]
// fn test_complex_struct() {
//     let original = ComplexStruct {
//         tuple_field: (1, 2, 3),
//         array_field: [(4, Some(5)), (6, None)],
//         reference: "hello",
//     };

//     let bytes = ComplexStruct::as_bytes(&original);
//     let deserialized = ComplexStruct::from_bytes(&bytes);
//     assert_eq!(original, deserialized);

//     let type_name = ComplexStruct::type_name();
//     let expected_name = "ComplexStruct {tuple_field: (u8, u16, u32), array_field: [(u8, Option<u16>); 2], reference: &str}";
//     assert_eq!(type_name.to_string(), expected_name);

//     let file = NamedTempFile::new().unwrap();
//     let db = Database::create(file.path()).unwrap();
//     const TABLE: TableDefinition<u32, ComplexStruct> = TableDefinition::new("test");

//     let write_txn = db.begin_write().unwrap();
//     {
//         let mut table = write_txn.open_table(TABLE).unwrap();
//         table.insert(1, &original).unwrap();
//     }
//     write_txn.commit().unwrap();

//     let read_txn = db.begin_read().unwrap();
//     let table = read_txn.open_table(TABLE).unwrap();
//     let retrieved = table.get(1).unwrap().unwrap();
//     assert_eq!(retrieved.value(), original);
// }
