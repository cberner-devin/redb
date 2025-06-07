use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Index, Lit, Meta};

#[proc_macro_derive(Value, attributes(redb))]
pub fn derive_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand_derive_value(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_derive_value(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;

    let type_name = extract_type_name(&input.attrs)?;

    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(fields) => &fields.unnamed,
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "Unit structs are not supported",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "Only structs are supported",
            ));
        }
    };

    if fields.is_empty() {
        return Err(syn::Error::new_spanned(
            struct_name,
            "Empty structs are not supported",
        ));
    }

    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let field_count = fields.len();

    let tuple_type = if field_count == 1 {
        let ty = &field_types[0];
        quote! { (#ty,) }
    } else {
        quote! { (#(#field_types),*) }
    };

    let is_named_fields = fields.iter().next().unwrap().ident.is_some();

    let (struct_to_tuple, tuple_to_struct) = if is_named_fields {
        let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
        let struct_to_tuple = if field_count == 1 {
            let field = &field_names[0];
            quote! { (value.#field.clone(),) }
        } else {
            quote! { (#(value.#field_names.clone()),*) }
        };

        let tuple_to_struct = {
            let indices: Vec<Index> = (0..field_count).map(Index::from).collect();
            quote! {
                #struct_name {
                    #(#field_names: tuple.#indices.clone()),*
                }
            }
        };
        (struct_to_tuple, tuple_to_struct)
    } else {
        let indices: Vec<Index> = (0..field_count).map(Index::from).collect();
        let struct_to_tuple = if field_count == 1 {
            quote! { (value.0.clone(),) }
        } else {
            quote! { (#(value.#indices.clone()),*) }
        };

        let tuple_to_struct = quote! {
            #struct_name(#(tuple.#indices.clone()),*)
        };
        (struct_to_tuple, tuple_to_struct)
    };

    let type_name_with_fields = {
        let field_type_names = field_types.iter().map(|ty| {
            quote! { stringify!(#ty) }
        });
        quote! {
            format!("{}({})", #type_name, [#(#field_type_names),*].join(","))
        }
    };

    let expanded = quote! {
        impl redb::Value for #struct_name {
            type SelfType<'a> = #struct_name where Self: 'a;
            type AsBytes<'a> = <#tuple_type as redb::Value>::AsBytes<'a> where Self: 'a;

            fn fixed_width() -> Option<usize> {
                <#tuple_type as redb::Value>::fixed_width()
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                let tuple = <#tuple_type as redb::Value>::from_bytes(data);
                #tuple_to_struct
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                let tuple = #struct_to_tuple;
                <#tuple_type as redb::Value>::as_bytes(&tuple)
            }

            fn type_name() -> redb::TypeName {
                redb::TypeName::new(&#type_name_with_fields)
            }
        }
    };

    Ok(expanded)
}

fn extract_type_name(attrs: &[syn::Attribute]) -> syn::Result<String> {
    for attr in attrs {
        if attr.path().is_ident("redb") {
            match &attr.meta {
                Meta::List(meta_list) => {
                    let nested = meta_list.parse_args::<syn::MetaNameValue>()?;
                    if nested.path.is_ident("type_name") {
                        if let syn::Expr::Lit(expr_lit) = &nested.value {
                            if let Lit::Str(lit_str) = &expr_lit.lit {
                                return Ok(lit_str.value());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing required attribute: #[redb(type_name = \"...\")]",
    ))
}
