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

// #[derive(Value, Debug)]
// struct ComplexStruct<'inner, 'inner2> {
//     tuple_field: (u8, u16, u32),
//     array_field: [(u8, Option<u16>); 2],
//     reference: &'inner str,
//     reference2: &'inner2 [u8],
// }

const SIMPLE_TABLE: TableDefinition<u32, SimpleStruct> = TableDefinition::new("simple");
const TUPLE_TABLE: TableDefinition<u32, TupleStruct> = TableDefinition::new("tuple");
const SINGLE_TABLE: TableDefinition<u32, SingleField> = TableDefinition::new("single");

#[test]
fn test_simple_struct() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };

    let bytes = SimpleStruct::as_bytes(&original);
    let deserialized = SimpleStruct::from_bytes(&bytes);
    assert_eq!(original, deserialized);

    let expected = "SimpleStruct {id: u32, name: String}";
    let type_name = SimpleStruct::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);

    let tmpfile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SIMPLE_TABLE).unwrap();
        table.insert(1, &original).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SIMPLE_TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    assert_eq!(original, retrieved.value());
}

#[test]
fn test_tuple_struct() {
    let original = TupleStruct(123456789, true);

    let bytes = TupleStruct::as_bytes(&original);
    let deserialized = TupleStruct::from_bytes(&bytes);
    assert_eq!(original, deserialized);

    let expected = "TupleStruct(u64, bool)";
    let type_name = TupleStruct::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);

    let tmpfile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TUPLE_TABLE).unwrap();
        table.insert(1, &original).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TUPLE_TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    assert_eq!(original, retrieved.value());
}

#[test]
fn test_single_field() {
    let original = SingleField { value: -42 };

    let bytes = SingleField::as_bytes(&original);
    let deserialized = SingleField::from_bytes(&bytes);
    assert_eq!(original, deserialized);

    let expected = "SingleField {value: i32}";
    let type_name = SingleField::type_name();
    let actual = type_name.name();
    assert_eq!(expected, actual);

    let tmpfile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SINGLE_TABLE).unwrap();
        table.insert(1, &original).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SINGLE_TABLE).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    assert_eq!(original, retrieved.value());
}

// #[test]
// fn test_complex_struct_compilation() {
//     let test_str = "hello";
//     let test_bytes = b"world";
//
//     let original = ComplexStruct {
//         tuple_field: (1, 2, 3),
//         array_field: [(4, Some(5)), (6, None)],
//         reference: test_str,
//         reference2: test_bytes,
//     };
//
//     let bytes = ComplexStruct::as_bytes(&original);
//     let _deserialized = ComplexStruct::from_bytes(&bytes);
//
//     let expected = "ComplexStruct {tuple_field: (u8, u16, u32), array_field: [(u8, Option<u16>); 2], reference: &str, reference2: &[u8]}";
//     let type_name = ComplexStruct::type_name();
//     let actual = type_name.name();
//     assert_eq!(expected, actual);
// }
