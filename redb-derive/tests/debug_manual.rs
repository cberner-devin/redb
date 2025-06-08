use redb::Value;

#[derive(Debug)]
struct TestStruct {
    id: u32,
    name: String,
}

impl Value for TestStruct {
    type SelfType<'a> = TestStruct
    where
        Self: 'a;
    
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;
    
    fn fixed_width() -> Option<usize> {
        None
    }
    
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let mut offset = 0;
        
        let name_len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
        offset += 4;
        
        let id = u32::from_bytes(&data[offset..offset+4]);
        offset += 4;
        
        let name = String::from_bytes(&data[offset..offset+name_len]);
        
        TestStruct { id, name }
    }
    
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut result = Vec::new();
        
        let name_bytes = String::as_bytes(&value.name);
        let name_len = name_bytes.as_ref().len() as u32;
        result.extend_from_slice(&name_len.to_le_bytes());
        
        let id_bytes = u32::as_bytes(&value.id);
        result.extend_from_slice(id_bytes.as_ref());
        
        result.extend_from_slice(name_bytes.as_ref());
        
        result
    }
    
    fn type_name() -> redb::TypeName {
        redb::TypeName::new("TestStruct {id: u32, name: String}")
    }
}

#[test]
fn test_manual_impl() {
    let original = TestStruct {
        id: 42,
        name: "test".to_string(),
    };

    let bytes = TestStruct::as_bytes(&original);
    let deserialized = TestStruct::from_bytes(&bytes);

    assert_eq!(original.id, deserialized.id);
    assert_eq!(original.name, deserialized.name);
}
