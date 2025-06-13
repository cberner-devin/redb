use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, GenericParam, Ident, parse_macro_input};

#[proc_macro_derive(Value)]
pub fn derive_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match generate_value_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_value_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Struct(data_struct) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "Value can only be derived for structs",
        ));
    };

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let self_type = generate_self_type(name, generics);

    let type_name_impl = generate_type_name(name, &data_struct.fields);
    let as_bytes_impl = generate_as_bytes(&data_struct.fields);
    let from_bytes_impl = generate_from_bytes(name, &data_struct.fields);
    let fixed_width_impl = generate_fixed_width(&data_struct.fields);

    Ok(quote! {
        impl #impl_generics redb::Value for #name #ty_generics #where_clause {
            type SelfType<'a> = #self_type
            where
                Self: 'a;
            type AsBytes<'a> = Vec<u8>
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                #fixed_width_impl
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                #from_bytes_impl
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                #as_bytes_impl
            }

            fn type_name() -> redb::TypeName {
                #type_name_impl
            }
        }
    })
}

fn generate_self_type(name: &syn::Ident, generics: &syn::Generics) -> proc_macro2::TokenStream {
    if generics.params.is_empty() {
        quote! { #name }
    } else {
        let params: Vec<_> = generics
            .params
            .iter()
            .map(|param| match param {
                GenericParam::Lifetime(_) => quote! { 'a },
                GenericParam::Type(type_param) => {
                    let ident = &type_param.ident;
                    quote! { #ident }
                }
                GenericParam::Const(const_param) => {
                    let ident = &const_param.ident;
                    quote! { #ident }
                }
            })
            .collect();

        quote! { #name<#(#params),*> }
    }
}

fn generate_type_name(struct_name: &Ident, fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_strings: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| {
                    let field_name = field.ident.as_ref().unwrap();
                    let field_type = &field.ty;
                    quote! {
                        format!("{}: {}", stringify!(#field_name), <#field_type>::type_name().name())
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
                        format!("{}", <#field_type>::type_name().name())
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

fn get_field_types(fields: &Fields) -> Vec<syn::Type> {
    match fields {
        Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .map(|field| &field.ty)
            .cloned()
            .collect(),
        Fields::Unnamed(fields_unnamed) => fields_unnamed
            .unnamed
            .iter()
            .map(|field| &field.ty)
            .cloned()
            .collect(),
        Fields::Unit => vec![],
    }
}

fn generate_fixed_width(fields: &Fields) -> proc_macro2::TokenStream {
    let field_types = get_field_types(fields);
    quote! {
        {
            let mut total_width = 0usize;
            #(
                total_width += <#field_types>::fixed_width()?;
            )*
            Some(total_width)
        }
    }
}

fn generate_as_bytes(fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_names: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| &field.ident)
                .collect();
            let field_types: Vec<_> = fields_named.named.iter().map(|field| &field.ty).collect();
            let num_fields = field_types.len();

            if num_fields == 0 {
                return quote! { Vec::new() };
            }

            if num_fields == 1 {
                let field_name = &field_names[0];
                let field_type = &field_types[0];
                quote! {
                    {
                        let field_bytes = <#field_type>::as_bytes(&value.#field_name);
                        field_bytes.as_ref().to_vec()
                    }
                }
            } else {
                let field_names_except_last = &field_names[..num_fields - 1];
                let field_types_except_last = &field_types[..num_fields - 1];

                quote! {
                    {
                        let mut result = Vec::new();

                        #(
                            if <#field_types_except_last>::fixed_width().is_none() {
                                let field_bytes = <#field_types_except_last>::as_bytes(&value.#field_names_except_last);
                                let bytes: &[u8] = field_bytes.as_ref();
                                let len = bytes.len();
                                if len < 254 {
                                    result.push(len.try_into().unwrap());
                                } else if len <= u16::MAX.into() {
                                    let u16_len: u16 = len.try_into().unwrap();
                                    result.push(254u8);
                                    result.extend_from_slice(&u16_len.to_le_bytes());
                                } else {
                                    let u32_len: u32 = len.try_into().unwrap();
                                    result.push(255u8);
                                    result.extend_from_slice(&u32_len.to_le_bytes());
                                }
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
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_types: Vec<_> = fields_unnamed
                .unnamed
                .iter()
                .map(|field| &field.ty)
                .collect();
            let field_indices: Vec<_> = (0..field_types.len()).map(syn::Index::from).collect();
            let num_fields = field_types.len();

            if num_fields == 0 {
                return quote! { Vec::new() };
            }

            if num_fields == 1 {
                let field_index = &field_indices[0];
                let field_type = &field_types[0];
                quote! {
                    {
                        let field_bytes = <#field_type>::as_bytes(&value.#field_index);
                        field_bytes.as_ref().to_vec()
                    }
                }
            } else {
                let field_types_except_last = &field_types[..num_fields - 1];
                let field_indices_except_last = &field_indices[..num_fields - 1];

                quote! {
                    {
                        let mut result = Vec::new();

                        #(
                            if <#field_types_except_last>::fixed_width().is_none() {
                                let field_bytes = <#field_types_except_last>::as_bytes(&value.#field_indices_except_last);
                                let bytes: &[u8] = field_bytes.as_ref();
                                let len = bytes.len();
                                if len < 254 {
                                    result.push(len.try_into().unwrap());
                                } else if len <= u16::MAX.into() {
                                    let u16_len: u16 = len.try_into().unwrap();
                                    result.push(254u8);
                                    result.extend_from_slice(&u16_len.to_le_bytes());
                                } else {
                                    let u32_len: u32 = len.try_into().unwrap();
                                    result.push(255u8);
                                    result.extend_from_slice(&u32_len.to_le_bytes());
                                }
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
        }
        Fields::Unit => {
            quote! { Vec::new() }
        }
    }
}

fn generate_from_bytes(name: &Ident, fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_names: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| &field.ident)
                .collect();
            let field_types: Vec<_> = fields_named.named.iter().map(|field| &field.ty).collect();
            let num_fields = field_types.len();

            if num_fields == 0 {
                return quote! { #name {} };
            }

            if num_fields == 1 {
                let field_name = &field_names[0];
                let field_type = &field_types[0];
                quote! {
                    {
                        let #field_name = <#field_type>::from_bytes(data);
                        #name {
                            #field_name
                        }
                    }
                }
            } else {
                let field_names_except_last = &field_names[..num_fields - 1];
                let field_types_except_last = &field_types[..num_fields - 1];
                let last_field_name = field_names.last();
                let last_field_type = field_types.last();

                quote! {
                    {
                        let mut offset = 0usize;
                        let mut var_lengths = Vec::new();

                        #(
                            if <#field_types_except_last>::fixed_width().is_none() {
                                let (len, bytes_read) = match data[offset] {
                                    0u8..=253u8 => (data[offset] as usize, 1usize),
                                    254u8 => (
                                        u16::from_le_bytes(data[offset + 1..offset + 3].try_into().unwrap()) as usize,
                                        3usize,
                                    ),
                                    255u8 => (
                                        u32::from_le_bytes(data[offset + 1..offset + 5].try_into().unwrap()) as usize,
                                        5usize,
                                    ),
                                };
                                var_lengths.push(len);
                                offset += bytes_read;
                            }
                        )*

                        let mut var_index = 0;
                        #(
                            let #field_names_except_last = if let Some(fixed_width) = <#field_types_except_last>::fixed_width() {
                                let field_data = &data[offset..offset + fixed_width];
                                offset += fixed_width;
                                <#field_types_except_last>::from_bytes(field_data)
                            } else {
                                let len = var_lengths[var_index];
                                let field_data = &data[offset..offset + len];
                                offset += len;
                                var_index += 1;
                                <#field_types_except_last>::from_bytes(field_data)
                            };
                        )*

                        let #last_field_name = if let Some(fixed_width) = <#last_field_type>::fixed_width() {
                            let field_data = &data[offset..offset + fixed_width];
                            <#last_field_type>::from_bytes(field_data)
                        } else {
                            <#last_field_type>::from_bytes(&data[offset..])
                        };

                        #name {
                            #(#field_names),*
                        }
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
            let num_fields = field_types.len();

            if num_fields == 0 {
                return quote! { #name() };
            }

            if num_fields == 1 {
                let field_var = &field_vars[0];
                let field_type = &field_types[0];
                quote! {
                    {
                        let #field_var = <#field_type>::from_bytes(data);
                        #name(#field_var)
                    }
                }
            } else {
                let field_types_except_last = &field_types[..num_fields - 1];
                let field_vars_except_last = &field_vars[..num_fields - 1];
                let last_field_var = field_vars.last();
                let last_field_type = field_types.last();

                quote! {
                    {
                        let mut offset = 0usize;
                        let mut var_lengths = Vec::new();

                        #(
                            if <#field_types_except_last>::fixed_width().is_none() {
                                let (len, bytes_read) = match data[offset] {
                                    0u8..=253u8 => (data[offset] as usize, 1usize),
                                    254u8 => (
                                        u16::from_le_bytes(data[offset + 1..offset + 3].try_into().unwrap()) as usize,
                                        3usize,
                                    ),
                                    255u8 => (
                                        u32::from_le_bytes(data[offset + 1..offset + 5].try_into().unwrap()) as usize,
                                        5usize,
                                    ),
                                };
                                var_lengths.push(len);
                                offset += bytes_read;
                            }
                        )*

                        let mut var_index = 0;
                        #(
                            let #field_vars_except_last = if let Some(fixed_width) = <#field_types_except_last>::fixed_width() {
                                let field_data = &data[offset..offset + fixed_width];
                                offset += fixed_width;
                                <#field_types_except_last>::from_bytes(field_data)
                            } else {
                                let len = var_lengths[var_index];
                                let field_data = &data[offset..offset + len];
                                offset += len;
                                var_index += 1;
                                <#field_types_except_last>::from_bytes(field_data)
                            };
                        )*

                        let #last_field_var = if let Some(fixed_width) = <#last_field_type>::fixed_width() {
                            let field_data = &data[offset..offset + fixed_width];
                            <#last_field_type>::from_bytes(field_data)
                        } else {
                            <#last_field_type>::from_bytes(&data[offset..])
                        };

                        #name(#(#field_vars),*)
                    }
                }
            }
        }
        Fields::Unit => {
            quote! { #name }
        }
    }
}
