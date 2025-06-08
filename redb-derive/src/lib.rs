use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(Value)]
pub fn derive_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = match &input.data {
        Data::Struct(data_struct) => {
            let type_name_impl = generate_type_name(&input, &data_struct.fields);
            let serialization_impl = generate_serialization(&data_struct.fields);
            let deserialization_impl = generate_deserialization(&data_struct.fields);
            let fixed_width_impl = generate_fixed_width(&data_struct.fields);

            quote! {
                impl #impl_generics redb::Value for #name #ty_generics #where_clause {
                    type SelfType<'a> = #name #ty_generics where Self: 'a;
                    type AsBytes<'a> = Vec<u8> where Self: 'a;

                    fn fixed_width() -> Option<usize> {
                        #fixed_width_impl
                    }

                    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
                    where
                        Self: 'a,
                    {
                        #deserialization_impl
                    }

                    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
                    where
                        Self: 'a,
                        Self: 'b,
                    {
                        #serialization_impl
                    }

                    fn type_name() -> redb::TypeName {
                        #type_name_impl
                    }
                }
            }
        }
        _ => {
            return syn::Error::new_spanned(&input, "Value can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    TokenStream::from(expanded)
}

fn generate_type_name(input: &DeriveInput, fields: &Fields) -> proc_macro2::TokenStream {
    let struct_name = &input.ident;

    match fields {
        Fields::Named(fields_named) => {
            let field_strings: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| {
                    let field_name = field.ident.as_ref().unwrap();
                    let field_type = &field.ty;
                    quote! {
                        format!("{}: {}", stringify!(#field_name), <#field_type>::type_name())
                    }
                })
                .collect();

            quote! {
                redb::TypeName::new(&format!("{} {{{}}}",
                    stringify!(#struct_name),
                    [#(#field_strings),*].join(", ")
                ))
            }
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_strings: Vec<_> = fields_unnamed
                .unnamed
                .iter()
                .map(|field| {
                    let field_type = &field.ty;
                    quote! {
                        format!("{}", <#field_type>::type_name())
                    }
                })
                .collect();

            quote! {
                redb::TypeName::new(&format!("{}({})",
                    stringify!(#struct_name),
                    [#(#field_strings),*].join(", ")
                ))
            }
        }
        Fields::Unit => {
            quote! {
                redb::TypeName::new(&format!("{}", stringify!(#struct_name)))
            }
        }
    }
}

fn generate_fixed_width(fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_types: Vec<_> = fields_named.named.iter().map(|field| &field.ty).collect();
            quote! {
                {
                    let mut total_width = 0usize;
                    #(
                        if let Some(width) = <#field_types>::fixed_width() {
                            total_width += width;
                        } else {
                            return None;
                        }
                    )*
                    Some(total_width)
                }
            }
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_types: Vec<_> = fields_unnamed
                .unnamed
                .iter()
                .map(|field| &field.ty)
                .collect();
            quote! {
                {
                    let mut total_width = 0usize;
                    #(
                        if let Some(width) = <#field_types>::fixed_width() {
                            total_width += width;
                        } else {
                            return None;
                        }
                    )*
                    Some(total_width)
                }
            }
        }
        Fields::Unit => {
            quote! { Some(0) }
        }
    }
}

fn generate_serialization(fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_names: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| &field.ident)
                .collect();
            let field_types: Vec<_> = fields_named.named.iter().map(|field| &field.ty).collect();

            quote! {
                {
                    let mut result = Vec::new();

                    #(
                        if <#field_types>::fixed_width().is_none() {
                            let field_bytes = <#field_types>::as_bytes(&value.#field_names);
                            let len = field_bytes.as_ref().len() as u32;
                            result.extend_from_slice(&len.to_le_bytes());
                        }
                    )*

                    #(
                        {
                            let field_bytes = <#field_types>::as_bytes(&value.#field_names);
                            result.extend_from_slice(field_bytes.as_ref());
                        }
                    )*

                    result
                }
            }
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_types: Vec<_> = fields_unnamed
                .unnamed
                .iter()
                .map(|field| &field.ty)
                .collect();
            let field_indices: Vec<_> = (0..field_types.len()).map(syn::Index::from).collect();

            quote! {
                {
                    let mut result = Vec::new();

                    #(
                        if <#field_types>::fixed_width().is_none() {
                            let field_bytes = <#field_types>::as_bytes(&value.#field_indices);
                            let len = field_bytes.as_ref().len() as u32;
                            result.extend_from_slice(&len.to_le_bytes());
                        }
                    )*

                    #(
                        {
                            let field_bytes = <#field_types>::as_bytes(&value.#field_indices);
                            result.extend_from_slice(field_bytes.as_ref());
                        }
                    )*

                    result
                }
            }
        }
        Fields::Unit => {
            quote! { Vec::new() }
        }
    }
}

fn generate_deserialization(fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_names: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| &field.ident)
                .collect();
            let field_types: Vec<_> = fields_named.named.iter().map(|field| &field.ty).collect();

            quote! {
                {
                    let mut offset = 0usize;
                    let mut var_lengths = Vec::new();

                    #(
                        if <#field_types>::fixed_width().is_none() {
                            let len = u32::from_le_bytes([
                                data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                            ]) as usize;
                            var_lengths.push(len);
                            offset += 4;
                        } else {
                            var_lengths.push(0); // placeholder for fixed-width fields
                        }
                    )*

                    let mut var_index = 0;
                    #(
                        let #field_names = if let Some(fixed_width) = <#field_types>::fixed_width() {
                            let field_data = &data[offset..offset + fixed_width];
                            offset += fixed_width;
                            <#field_types>::from_bytes(field_data)
                        } else {
                            let len = var_lengths[var_index];
                            var_index += 1;
                            let field_data = &data[offset..offset + len];
                            offset += len;
                            <#field_types>::from_bytes(field_data)
                        };
                    )*

                    Self {
                        #(#field_names),*
                    }
                }
            }
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_types: Vec<_> = fields_unnamed
                .unnamed
                .iter()
                .map(|field| &field.ty)
                .collect();
            let field_vars: Vec<_> = (0..field_types.len())
                .map(|i| quote::format_ident!("field_{}", i))
                .collect();

            quote! {
                {
                    let mut offset = 0usize;
                    let mut var_lengths = Vec::new();

                    #(
                        if <#field_types>::fixed_width().is_none() {
                            let len = u32::from_le_bytes([
                                data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                            ]) as usize;
                            var_lengths.push(len);
                            offset += 4;
                        } else {
                            var_lengths.push(0); // placeholder for fixed-width fields
                        }
                    )*

                    let mut var_index = 0;
                    #(
                        let #field_vars = if let Some(fixed_width) = <#field_types>::fixed_width() {
                            let field_data = &data[offset..offset + fixed_width];
                            offset += fixed_width;
                            <#field_types>::from_bytes(field_data)
                        } else {
                            let len = var_lengths[var_index];
                            var_index += 1;
                            let field_data = &data[offset..offset + len];
                            offset += len;
                            <#field_types>::from_bytes(field_data)
                        };
                    )*

                    Self(#(#field_vars),*)
                }
            }
        }
        Fields::Unit => {
            quote! { Self }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{Database, TableDefinition, TypeName, Value};
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
    struct ComplexStruct<'a> {
        tuple_field: (u8, u16, u32),
        array_field: [(u8, Option<u16>); 2],
        reference: &'a str,
    }

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

    #[test]
    fn test_complex_struct() {
        let original = ComplexStruct {
            tuple_field: (1, 2, 3),
            array_field: [(4, Some(5)), (6, None)],
            reference: "hello",
        };

        let bytes = ComplexStruct::as_bytes(&original);
        let deserialized = ComplexStruct::from_bytes(&bytes);
        assert_eq!(original, deserialized);

        let type_name = ComplexStruct::type_name();
        let expected_name = "ComplexStruct {tuple_field: (u8, u16, u32), array_field: [(u8, Option<u16>); 2], reference: &str}";
        assert_eq!(type_name.to_string(), expected_name);

        let file = NamedTempFile::new().unwrap();
        let db = Database::create(file.path()).unwrap();
        const TABLE: TableDefinition<u32, ComplexStruct> = TableDefinition::new("test");

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
}
