use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type, GenericParam, PathArguments, GenericArgument};

#[proc_macro_derive(Value)]
pub fn derive_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    match generate_value_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_value_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let generics = &input.generics;
    
    let self_type = generate_self_type(name, generics);
    
    let type_name_impl = generate_type_name(input)?;
    
    let (fixed_width_impl, from_bytes_impl, as_bytes_impl) = generate_serialization(input)?;
    
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

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

fn generate_self_type(name: &syn::Ident, generics: &syn::Generics) -> TokenStream2 {
    if generics.params.is_empty() {
        quote! { #name }
    } else {
        let params: Vec<_> = generics.params.iter().map(|param| {
            match param {
                GenericParam::Lifetime(_) => quote! { 'a },
                GenericParam::Type(type_param) => {
                    let ident = &type_param.ident;
                    quote! { #ident }
                }
                GenericParam::Const(const_param) => {
                    let ident = &const_param.ident;
                    quote! { #ident }
                }
            }
        }).collect();
        
        quote! { #name<#(#params),*> }
    }
}

fn generate_type_name(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    
    match &input.data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields) => {
                    let field_strings: Vec<_> = fields.named.iter().map(|field| {
                        let field_name = field.ident.as_ref().unwrap();
                        let field_type = &field.ty;
                        let type_name = type_to_string(field_type);
                        format!("{}: {}", field_name, type_name)
                    }).collect();
                    
                    let type_name_str = format!("{} {{{}}}", name, field_strings.join(", "));
                    Ok(quote! {
                        redb::TypeName::new(&#type_name_str)
                    })
                }
                Fields::Unnamed(fields) => {
                    let field_strings: Vec<_> = fields.unnamed.iter().map(|field| {
                        let field_type = &field.ty;
                        type_to_string(field_type)
                    }).collect();
                    
                    let type_name_str = format!("{}({})", name, field_strings.join(", "));
                    Ok(quote! {
                        redb::TypeName::new(&#type_name_str)
                    })
                }
                Fields::Unit => {
                    let type_name_str = name.to_string();
                    Ok(quote! {
                        redb::TypeName::new(&#type_name_str)
                    })
                }
            }
        }
        _ => Err(syn::Error::new_spanned(input, "Value derive only supports structs")),
    }
}

fn type_to_string(ty: &Type) -> String {
    use quote::ToTokens;
    
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                let base_name = segment.ident.to_string();
                
                match &segment.arguments {
                    PathArguments::AngleBracketed(args) => {
                        let arg_strings: Vec<String> = args.args.iter().map(|arg| {
                            match arg {
                                GenericArgument::Type(ty) => type_to_string(ty),
                                GenericArgument::Lifetime(_) => "'_".to_string(),
                                _ => "?".to_string(),
                            }
                        }).collect();
                        
                        if arg_strings.is_empty() {
                            base_name
                        } else {
                            format!("{}<{}>", base_name, arg_strings.join(", "))
                        }
                    }
                    _ => base_name,
                }
            } else {
                "Unknown".to_string()
            }
        }
        Type::Reference(type_ref) => {
            let elem_str = type_to_string(&type_ref.elem);
            if type_ref.mutability.is_some() {
                format!("&mut {}", elem_str)
            } else {
                format!("&{}", elem_str)
            }
        }
        Type::Array(type_array) => {
            let elem_type = type_to_string(&type_array.elem);
            match &type_array.len {
                syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Int(lit_int), .. }) => {
                    format!("[{}; {}]", elem_type, lit_int.base10_digits())
                }
                _ => format!("[{}; N]", elem_type),
            }
        }
        Type::Tuple(type_tuple) => {
            let elem_strings: Vec<String> = type_tuple.elems.iter().map(type_to_string).collect();
            format!("({})", elem_strings.join(", "))
        }
        Type::Slice(type_slice) => {
            format!("[{}]", type_to_string(&type_slice.elem))
        }
        _ => {
            ty.to_token_stream().to_string()
        }
    }
}

fn replace_lifetimes_with_a(ty: &syn::Type) -> syn::Type {
    use syn::{Type, PathArguments, GenericArgument, Lifetime};
    
    match ty {
        Type::Reference(type_ref) => {
            let mut new_ref = type_ref.clone();
            new_ref.lifetime = Some(Lifetime::new("'a", proc_macro2::Span::call_site()));
            new_ref.elem = Box::new(replace_lifetimes_with_a(&type_ref.elem));
            Type::Reference(new_ref)
        }
        Type::Path(type_path) => {
            let mut new_path = type_path.clone();
            for segment in &mut new_path.path.segments {
                if let PathArguments::AngleBracketed(ref mut args) = segment.arguments {
                    for arg in &mut args.args {
                        match arg {
                            GenericArgument::Lifetime(lifetime) => {
                                *lifetime = Lifetime::new("'a", proc_macro2::Span::call_site());
                            }
                            GenericArgument::Type(ty) => {
                                *ty = replace_lifetimes_with_a(ty);
                            }
                            _ => {}
                        }
                    }
                }
            }
            Type::Path(new_path)
        }
        Type::Array(type_array) => {
            let mut new_array = type_array.clone();
            new_array.elem = Box::new(replace_lifetimes_with_a(&type_array.elem));
            Type::Array(new_array)
        }
        Type::Tuple(type_tuple) => {
            let mut new_tuple = type_tuple.clone();
            for elem in &mut new_tuple.elems {
                *elem = replace_lifetimes_with_a(elem);
            }
            Type::Tuple(new_tuple)
        }
        Type::Slice(type_slice) => {
            let mut new_slice = type_slice.clone();
            new_slice.elem = Box::new(replace_lifetimes_with_a(&type_slice.elem));
            Type::Slice(new_slice)
        }
        _ => ty.clone(),
    }
}

fn generate_serialization(input: &DeriveInput) -> syn::Result<(TokenStream2, TokenStream2, TokenStream2)> {
    match &input.data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields) => generate_named_fields_serialization(&fields.named),
                Fields::Unnamed(fields) => generate_unnamed_fields_serialization(&fields.unnamed),
                Fields::Unit => Ok(generate_unit_serialization()),
            }
        }
        _ => Err(syn::Error::new_spanned(input, "Value derive only supports structs")),
    }
}

fn generate_named_fields_serialization(fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>) -> syn::Result<(TokenStream2, TokenStream2, TokenStream2)> {
    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_types: Vec<_> = fields.iter().map(|f| f.ty.clone()).collect();
    
    let fixed_width_impl = quote! {
        None
    };
    
    let as_bytes_impl = quote! {
        {
            let mut result = Vec::new();
            
            #(
                if #field_types::fixed_width().is_none() {
                    let field_bytes = #field_types::as_bytes(&value.#field_names);
                    let len = field_bytes.as_ref().len() as u32;
                    result.extend_from_slice(&len.to_le_bytes());
                }
            )*
            
            #(
                let field_bytes = #field_types::as_bytes(&value.#field_names);
                result.extend_from_slice(field_bytes.as_ref());
            )*
            
            result
        }
    };
    
    let from_bytes_impl = quote! {
        {
            let mut offset = 0;
            
            let mut lengths = Vec::new();
            #(
                if #field_types::fixed_width().is_none() {
                    let len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
                    lengths.push(len);
                    offset += 4;
                }
            )*
            
            let mut length_idx = 0;
            #(
                let #field_names = if let Some(width) = #field_types::fixed_width() {
                    let field_data = &data[offset..offset+width];
                    offset += width;
                    #field_types::from_bytes(field_data)
                } else {
                    let len = lengths[length_idx];
                    length_idx += 1;
                    let field_data = &data[offset..offset+len];
                    offset += len;
                    #field_types::from_bytes(field_data)
                };
            )*
            
            Self { #(#field_names),* }
        }
    };
    
    Ok((fixed_width_impl, from_bytes_impl, as_bytes_impl))
}

fn generate_unnamed_fields_serialization(fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>) -> syn::Result<(TokenStream2, TokenStream2, TokenStream2)> {
    let field_indices: Vec<_> = (0..fields.len()).map(syn::Index::from).collect();
    let field_types: Vec<_> = fields.iter().map(|f| f.ty.clone()).collect();
    
    let fixed_width_impl = quote! {
        None
    };
    
    let as_bytes_impl = quote! {
        {
            let mut result = Vec::new();
            
            #(
                if <#field_types as redb::Value>::fixed_width().is_none() {
                    let field_bytes = <#field_types as redb::Value>::as_bytes(&value.#field_indices);
                    let len = field_bytes.as_ref().len() as u32;
                    result.extend_from_slice(&len.to_le_bytes());
                }
            )*
            
            #(
                let field_bytes = <#field_types as redb::Value>::as_bytes(&value.#field_indices);
                result.extend_from_slice(field_bytes.as_ref());
            )*
            
            result
        }
    };
    
    let field_vars: Vec<_> = (0..field_types.len()).map(|i| {
        syn::Ident::new(&format!("field_{}", i), proc_macro2::Span::call_site())
    }).collect();
    
    let from_bytes_impl = quote! {
        {
            let mut offset = 0;
            
            let mut lengths = Vec::new();
            #(
                if <#field_types as redb::Value>::fixed_width().is_none() {
                    let len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
                    lengths.push(len);
                    offset += 4;
                }
            )*
            
            let mut length_idx = 0;
            #(
                let #field_vars = if let Some(width) = <#field_types as redb::Value>::fixed_width() {
                    let field_data = &data[offset..offset+width];
                    offset += width;
                    <#field_types as redb::Value>::from_bytes(field_data)
                } else {
                    let len = lengths[length_idx];
                    length_idx += 1;
                    let field_data = &data[offset..offset+len];
                    offset += len;
                    <#field_types as redb::Value>::from_bytes(field_data)
                };
            )*
            
            Self(#(#field_vars),*)
        }
    };
    
    Ok((fixed_width_impl, from_bytes_impl, as_bytes_impl))
}

fn generate_unit_serialization() -> (TokenStream2, TokenStream2, TokenStream2) {
    let fixed_width_impl = quote! { Some(0) };
    let from_bytes_impl = quote! { Self };
    let as_bytes_impl = quote! { Vec::new() };
    
    (fixed_width_impl, from_bytes_impl, as_bytes_impl)
}
