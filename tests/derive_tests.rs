#[cfg(feature = "derive")]
use redb::{Database, Key, TableDefinition};
#[cfg(feature = "derive")]
use std::borrow::Borrow;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "derive", derive(redb::Key))]
pub struct SimpleStruct {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "derive", derive(redb::Key))]
pub struct SingleField {
    pub value: u64,
}

impl redb::Value for SimpleStruct {
    type SelfType<'a>
        = SimpleStruct
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> SimpleStruct
    where
        Self: 'a,
    {
        let id_bytes = &data[0..4];
        let id = u32::from_le_bytes(id_bytes.try_into().unwrap());
        let name_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let name_bytes = &data[8..8 + name_len];
        let name = String::from_utf8(name_bytes.to_vec()).unwrap();
        SimpleStruct { id, name }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = Vec::new();
        result.extend_from_slice(&value.id.to_le_bytes());
        let name_bytes = value.name.as_bytes();
        result.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        result.extend_from_slice(name_bytes);
        result
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("SimpleStruct")
    }
}

impl redb::Value for SingleField {
    type SelfType<'a>
        = SingleField
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> SingleField
    where
        Self: 'a,
    {
        let value = u64::from_le_bytes(data.try_into().unwrap());
        SingleField { value }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'b,
    {
        value.value.to_le_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("SingleField")
    }
}

#[cfg(feature = "derive")]
fn test_helper<K>(values: &[K]) -> Result<(), Box<dyn std::error::Error>>
where
    K: redb::Key + redb::Value + Clone + std::fmt::Debug + 'static,
    for<'a> K: Borrow<K::SelfType<'a>>,
{
    let file = tempfile::NamedTempFile::new()?;
    let db = Database::create(file.path())?;

    let table_def: TableDefinition<K, u32> = TableDefinition::new("test_table");

    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(table_def)?;
        for (i, value) in values.iter().enumerate() {
            table.insert(value.clone(), &(i as u32))?;
        }
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(table_def)?;

    for (i, value) in values.iter().enumerate() {
        let retrieved = table.get(value.clone())?.unwrap();
        assert_eq!(retrieved.value(), i as u32);
    }

    Ok(())
}

#[cfg(feature = "derive")]
#[test]
fn test_simple_struct_ordering() {
    let struct1 = SimpleStruct {
        id: 1,
        name: "apple".to_string(),
    };
    let struct2 = SimpleStruct {
        id: 1,
        name: "banana".to_string(),
    };
    let struct3 = SimpleStruct {
        id: 2,
        name: "apple".to_string(),
    };

    assert_eq!(
        SimpleStruct::compare(
            &<SimpleStruct as redb::Value>::as_bytes(&struct1),
            &<SimpleStruct as redb::Value>::as_bytes(&struct2)
        ),
        struct1.cmp(&struct2)
    );
    assert_eq!(
        SimpleStruct::compare(
            &<SimpleStruct as redb::Value>::as_bytes(&struct1),
            &<SimpleStruct as redb::Value>::as_bytes(&struct3)
        ),
        struct1.cmp(&struct3)
    );
    assert_eq!(
        SimpleStruct::compare(
            &<SimpleStruct as redb::Value>::as_bytes(&struct2),
            &<SimpleStruct as redb::Value>::as_bytes(&struct3)
        ),
        struct2.cmp(&struct3)
    );
}

#[cfg(feature = "derive")]
#[test]
fn test_single_field_ordering() {
    let field1 = SingleField { value: 10 };
    let field2 = SingleField { value: 20 };
    let field3 = SingleField { value: 5 };

    assert_eq!(
        SingleField::compare(
            &<SingleField as redb::Value>::as_bytes(&field1),
            &<SingleField as redb::Value>::as_bytes(&field2)
        ),
        field1.cmp(&field2)
    );
    assert_eq!(
        SingleField::compare(
            &<SingleField as redb::Value>::as_bytes(&field1),
            &<SingleField as redb::Value>::as_bytes(&field3)
        ),
        field1.cmp(&field3)
    );
    assert_eq!(
        SingleField::compare(
            &<SingleField as redb::Value>::as_bytes(&field2),
            &<SingleField as redb::Value>::as_bytes(&field3)
        ),
        field2.cmp(&field3)
    );
}

#[cfg(feature = "derive")]
#[test]
fn test_simple_struct_database_operations() -> Result<(), Box<dyn std::error::Error>> {
    let values = vec![
        SimpleStruct {
            id: 1,
            name: "apple".to_string(),
        },
        SimpleStruct {
            id: 1,
            name: "banana".to_string(),
        },
        SimpleStruct {
            id: 2,
            name: "apple".to_string(),
        },
        SimpleStruct {
            id: 3,
            name: "cherry".to_string(),
        },
    ];

    test_helper(&values)?;
    Ok(())
}

#[cfg(feature = "derive")]
#[test]
fn test_single_field_database_operations() -> Result<(), Box<dyn std::error::Error>> {
    let values = vec![
        SingleField { value: 10 },
        SingleField { value: 5 },
        SingleField { value: 20 },
        SingleField { value: 1 },
    ];

    test_helper(&values)?;
    Ok(())
}
