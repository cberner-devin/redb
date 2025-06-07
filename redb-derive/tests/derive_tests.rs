use redb::{Database, TableDefinition, Value};
use redb_derive::Value as DeriveValue;
use tempfile::NamedTempFile;

#[derive(Debug, DeriveValue)]
struct SimpleStruct {
    id: u32,
    name: String,
}

#[derive(Debug, DeriveValue)]
struct TupleStruct(u64, bool);

#[derive(Debug, DeriveValue)]
struct SingleField {
    value: i32,
}

#[derive(Debug, DeriveValue)]
struct MixedTypes {
    fixed: u16,
    variable: String,
    optional: Option<u8>,
}

fn create_tempfile() -> NamedTempFile {
    NamedTempFile::new().unwrap()
}

#[test]
fn test_simple_struct_type_name() {
    assert_eq!(
        "SimpleStruct {id: u32, name: String}",
        format!("{}", SimpleStruct::type_name())
    );
}

#[test]
fn test_tuple_struct_type_name() {
    assert_eq!(
        "TupleStruct(u64,bool)",
        format!("{}", TupleStruct::type_name())
    );
}

#[test]
fn test_single_field_type_name() {
    assert_eq!(
        "SingleField {value: i32}",
        format!("{}", SingleField::type_name())
    );
}

#[test]
fn test_mixed_types_type_name() {
    assert_eq!(
        "MixedTypes {fixed: u16, variable: String, optional: Option<u8>}",
        format!("{}", MixedTypes::type_name())
    );
}

#[test]
fn test_simple_struct_serialization() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };

    let bytes = SimpleStruct::as_bytes(&original);
    let deserialized = SimpleStruct::from_bytes(bytes.as_ref());

    assert_eq!(deserialized.id, 42);
    assert_eq!(deserialized.name, "test");
}

#[test]
fn test_tuple_struct_serialization() {
    let original = TupleStruct(123, true);

    let bytes = TupleStruct::as_bytes(&original);
    let deserialized = TupleStruct::from_bytes(bytes.as_ref());

    assert_eq!(deserialized.0, 123);
    assert_eq!(deserialized.1, true);
}

#[test]
fn test_single_field_serialization() {
    let original = SingleField { value: -456 };

    let bytes = SingleField::as_bytes(&original);
    let deserialized = SingleField::from_bytes(bytes.as_ref());

    assert_eq!(deserialized.value, -456);
}

#[test]
fn test_mixed_types_serialization() {
    let original = MixedTypes {
        fixed: 1000,
        variable: "hello world".to_string(),
        optional: Some(255),
    };

    let bytes = MixedTypes::as_bytes(&original);
    let deserialized = MixedTypes::from_bytes(bytes.as_ref());

    assert_eq!(deserialized.fixed, 1000);
    assert_eq!(deserialized.variable, "hello world");
    assert_eq!(deserialized.optional, Some(255));
}

#[test]
fn test_fixed_width() {
    assert_eq!(SingleField::fixed_width(), Some(4));
    assert_eq!(SimpleStruct::fixed_width(), None);
    assert_eq!(TupleStruct::fixed_width(), Some(9));
    assert_eq!(MixedTypes::fixed_width(), None);
}

#[test]
fn test_database_integration() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<u32, SimpleStruct> = TableDefinition::new("test_table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        let value = SimpleStruct {
            id: 100,
            name: "database test".to_string(),
        };
        table.insert(&1, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    let retrieved = table.get(&1).unwrap().unwrap();

    assert_eq!(retrieved.value().id, 100);
    assert_eq!(retrieved.value().name, "database test");
}

#[test]
fn test_tuple_struct_database_integration() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<u32, TupleStruct> = TableDefinition::new("tuple_table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        let value = TupleStruct(999, false);
        table.insert(&1, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    let retrieved = table.get(&1).unwrap().unwrap();

    assert_eq!(retrieved.value().0, 999);
    assert_eq!(retrieved.value().1, false);
}

#[test]
fn test_vec_tuple_struct_database_integration() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<u32, Vec<TupleStruct>> = TableDefinition::new("vec_tuple_table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        let values = vec![
            TupleStruct(100, true),
            TupleStruct(200, false),
            TupleStruct(300, true),
        ];
        table.insert(&1, &values).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    let retrieved = table.get(&1).unwrap().unwrap();
    let vec_values = retrieved.value();

    assert_eq!(vec_values.len(), 3);
    assert_eq!(vec_values[0].0, 100);
    assert_eq!(vec_values[0].1, true);
    assert_eq!(vec_values[1].0, 200);
    assert_eq!(vec_values[1].1, false);
    assert_eq!(vec_values[2].0, 300);
    assert_eq!(vec_values[2].1, true);
}

#[test]
fn test_roundtrip_consistency() {
    let values = vec![
        SimpleStruct {
            id: 0,
            name: String::new(),
        },
        SimpleStruct {
            id: u32::MAX,
            name: "max".to_string(),
        },
        SimpleStruct {
            id: 12345,
            name: "ascii_test".to_string(),
        },
    ];

    for original in values {
        let bytes = SimpleStruct::as_bytes(&original);
        let deserialized = SimpleStruct::from_bytes(bytes.as_ref());

        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
    }
}
