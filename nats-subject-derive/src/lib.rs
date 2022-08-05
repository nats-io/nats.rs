use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, LitStr};

#[proc_macro_derive(ToSubject, attributes(subject))]
pub fn derive_to_subject(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let type_ident = input.ident;
    let sub_attr = input
        .attrs
        .iter()
        .find(|attr| attr.path.is_ident("subject"))
        .unwrap();
    let subject_template = sub_attr
        .parse_args::<LitStr>()
        .expect("#[subject(...)] must hold a string literal");

    quote! {
        impl ::async_nats::ToSubject for #type_ident {
            fn to_subject(&self) -> Result<::async_nats::SubjectBuf, ::async_nats::subject::Error> {
                ::async_nats::subj!(#subject_template)
            }
        }
    }
    .into()
}
