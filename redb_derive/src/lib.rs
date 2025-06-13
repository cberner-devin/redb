use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(Key)]
pub fn derive_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;

    let _fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(fields) => &fields.unnamed,
            Fields::Unit => {
                return syn::Error::new_spanned(
                    &input.ident,
                    "Key derive macro cannot be used on unit structs",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "Key derive macro can only be used on structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let expanded = quote! {
        impl redb::Key for #name {
            fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
                fn _assert_ord<T: Ord>() {}
                _assert_ord::<#name>();

                let value1 = <#name as redb::Value>::from_bytes(data1);
                let value2 = <#name as redb::Value>::from_bytes(data2);
                value1.cmp(&value2)
            }
        }
    };

    TokenStream::from(expanded)
}
