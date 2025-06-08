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

            let mut lifetime_params = Vec::new();
            let mut type_params = Vec::new();
            let mut const_params = Vec::new();

            for param in &generics.params {
                match param {
                    syn::GenericParam::Lifetime(lt) => lifetime_params.push(lt),
                    syn::GenericParam::Type(ty) => type_params.push(ty),
                    syn::GenericParam::Const(ct) => const_params.push(ct),
                }
            }

            let (type_generics_with_a, self_type_lifetime) = if lifetime_params.is_empty()
                && type_params.is_empty()
                && const_params.is_empty()
            {
                (quote! {}, quote! { 'a })
            } else {
                let mut params = Vec::new();

                let self_type_lifetime = if !lifetime_params.is_empty() {
                    let first_lifetime = &lifetime_params[0].lifetime;
                    params.push(quote! { #first_lifetime });
                    quote! { #first_lifetime }
                } else {
                    params.push(quote! { 'a });
                    quote! { 'a }
                };

                params.extend(type_params.iter().map(|tp| {
                    let ident = &tp.ident;
                    quote! { #ident }
                }));
                params.extend(const_params.iter().map(|cp| {
                    let ident = &cp.ident;
                    quote! { #ident }
                }));
                (quote! { < #(#params),* > }, self_type_lifetime)
            };

            let self_type_def = quote! { type SelfType<#self_type_lifetime> = #name #type_generics_with_a where Self: #self_type_lifetime; };

            quote! {
                impl #impl_generics redb::Value for #name #ty_generics #where_clause {
                    #self_type_def
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

                    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
                    where
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
                            let field_data = &data[offset..offset + len];
                            offset += len;
                            var_index += 1;
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
                            let field_data = &data[offset..offset + len];
                            offset += len;
                            var_index += 1;
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
