use proc_macro::TokenStream;
use quote::ToTokens;
use syn::{parse::Parse, punctuated::Punctuated, token};

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate syn;
#[macro_use]
extern crate quote;

struct GenericTuple {
    _paren_token: token::Paren,
    idents: Punctuated<syn::Ident, Token![,]>,
}

impl ToTokens for GenericTuple {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let idents = self.idents.iter();

        quote!((#(#idents),*)).to_tokens(tokens)
    }
}

impl Parse for GenericTuple {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content;

        Ok(GenericTuple {
            _paren_token: parenthesized!(content in input),
            idents: content.parse_terminated(syn::Ident::parse)?,
        })
    }
}

#[proc_macro]
pub fn impl_tuple_prefix(item: TokenStream) -> TokenStream {
    let tuple_type = parse_macro_input!(item as GenericTuple);
    let mut tuple_elements = tuple_type.idents.iter().collect::<Vec<_>>();

    let mut token_stream = proc_macro2::TokenStream::new();

    loop {
        tuple_elements.pop();
        if tuple_elements.is_empty() {
            break;
        }

        let generic_params = tuple_type.idents.iter().collect::<Vec<_>>();

        let tokens = quote! {
            impl<#(#generic_params),*> PrefixKey<#tuple_type> for (#(#tuple_elements),*,) where #(#generic_params: EncodeKey),* {}
        };

        tokens.to_tokens(&mut token_stream);
    }

    token_stream.into()
}

/*

enum BobsledAttribute {
    Key,
    SerializeWith(syn::Path),
    DeserializeWith(syn::Path)
}

impl Parse for BobsledAttribute {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {

        let content;
        parenthesized!(content in input);

        if content.peek(Ident::peek_any) {
            let ident: Ident = content.parse()?;

            if ident == "key" {
                return Ok(Self::Key);
            }
        }

        Err(input.error("Unsupported bobsled attribute"))
    }
}

#[proc_macro_derive(Record, attributes(bobsled))]
pub fn derive_record(item: TokenStream) -> TokenStream {
    let bobsled_ident = Ident::new("bobsled", Span::call_site());
    let DeriveInput {
        attrs,
        vis,
        ident,
        generics,
        data,
    } = parse_macro_input!(item as DeriveInput);

    let DataStruct { fields, .. } = match data {
        Data::Struct(data) => data,
        _ => panic!("Can only derive Record on structs"),
    };

    let fields = match fields {
        Fields::Named(fields) => fields.named,
        Fields::Unnamed(fields) => fields.unnamed,
        Fields::Unit => panic!("Struct cannot be a unit struct"),
    };

    let (key_fields, value_fields): (Vec<_>, Vec<_>) = fields
        .into_iter()
        .partition(|f| {
            f.attrs.iter().any(|a| {
                if a.path.is_ident(&bobsled_ident) {
                    if let Ok(attr) = syn::parse2::<BobsledAttribute>(a.tokens.clone()) {
                        if let BobsledAttribute::Key = attr {
                            return true;
                        }
                    }
                }

                false
            })
        });

    let key_types = key_fields.iter().map(|field| &field.ty).collect::<Vec<_>>();

    let tokens = quote! {
        const _: () = {
            #[derive(Debug)]
            enum RecordLoadError<S: ::bobsled::Store> {
                StoreError(S::Error),
                PrimaryKeyDecodeError(<<#ident as ::bobsled::Record<S>>::Key as ::bobsled::DecodeKey>::Error),
                ValueDecodeError(::bobsled::bincode::Error)
            }

            impl<S: ::bobsled::Store> ::std::fmt::Display for RecordLoadError<S> {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    match self {
                        Self::StoreError(err) => ::std::fmt::Display::fmt(err, f),
                        Self::PrimaryKeyDecodeError(err) => ::std::fmt::Display::fmt(err, f),
                        Self::ValueDecodeError(err) => ::std::fmt::Display::fmt(err, f),
                    }
                }
            }

            struct RecordIter<S: ::bobsled::Store> {
                pub iter: S::Iter
            }

            impl<S: ::bobsled::Store> Iterator for RecordIter<S> {
                type Item = Result<#ident, RecordLoadError<S>>;

                fn next(&mut self) -> Option<Self::Item> {
                    match self.iter.next()? {
                        Ok((key, value)) => match <<#ident as ::bobsled::Record<S>>::Key as ::bobsled::DecodeKey>::try_decode(key.as_ref()) {
                            Ok((key, _)) => Some(try_decode::<S>(key, value.as_ref())),
                            Err(err) => Some(Err(RecordLoadError::PrimaryKeyDecodeError(err))),
                        },
                        Err(err) => Some(Err(RecordLoadError::StoreError(err)))
                    }
                }
            }

            fn try_decode<S: ::bobsled::Store>(key: <#ident as ::bobsled::Record<S>>::Key, value: &[u8]) -> Result<#ident, <#ident as ::bobsled::Record<S>>::Error> {
                Ok(#ident { id: key.0, key: key.1 })
            }

            impl<S: ::bobsled::Store> ::bobsled::Record<S> for #ident {
                type Key = (#(#key_types),*);
                type Error = RecordLoadError<S>;
                type ScanIter = RecordIter<S>;

                fn fetch(store: &S, key: &Self::Key) -> Result<Option<Self>, Self::Error> {
                    match store.fetch(::bobsled::EncodeKey::encode(key).as_ref()) {
                        Ok(Some(value)) => Ok(Some(try_decode::<S>(key.clone(), value.as_ref())?)),
                        Ok(None) => Ok(None),
                        Err(err) => Err(Self::Error::StoreError(err))
                    }
                }

                fn scan(store: &S) -> Self::ScanIter {
                    Self::ScanIter {
                        iter: store.range(..)
                    }

                }

                fn scan_range<K: ::bobsled::PrefixKey<Self::Key>>(store: &S, range: impl ::std::ops::RangeBounds<K>) -> Self::ScanIter {
                    use ::bobsled::EncodeKey;
                    use ::std::ops::Bound;

                    let start = range.start_bound();
                    let end = range.end_bound();

                    let start = match start {
                        Bound::Included(bound) => Bound::Included(bound.encode()),
                        Bound::Excluded(bound) => Bound::Excluded(bound.encode()),
                        Bound::Unbounded => Bound::Unbounded,
                    };

                    let start = match start {
                        Bound::Included(ref bound) => Bound::Included(bound.as_ref()),
                        Bound::Excluded(ref bound) => Bound::Excluded(bound.as_ref()),
                        Bound::Unbounded => Bound::Unbounded,
                    };

                    let end = match end {
                        Bound::Included(bound) => Bound::Included(bound.encode()),
                        Bound::Excluded(bound) => Bound::Excluded(bound.encode()),
                        Bound::Unbounded => Bound::Unbounded,
                    };

                    let end = match end {
                        Bound::Included(ref bound) => Bound::Included(bound.as_ref()),
                        Bound::Excluded(ref bound) => Bound::Excluded(bound.as_ref()),
                        Bound::Unbounded => Bound::Unbounded,
                    };

                    Self::ScanIter {
                        iter: store.range((start, end))
                    }
                }

                fn scan_prefix(store: &S, prefix: &impl ::bobsled::PrefixKey<Self::Key>) -> Self::ScanIter {
                    use ::bobsled::EncodeKey;
                    use ::std::ops::Bound;

                    // Perform a prefix scan by constructing a range based on the prefix key and scanning that
                    // Requires an allocation (but tbf so does sled)

                    let prefix = prefix.encode();
                    let start = prefix.as_ref();

                    let mut end = start.to_owned();
                    while let Some(last) = end.pop() {
                        if last < u8::MAX {
                            end.push(last + 1);
                            return Self::ScanIter {
                                iter: store.range((Bound::Included(start), Bound::Included(end.as_ref()))),
                            };
                        }
                    }

                    Self::ScanIter {
                        iter: store.range((Bound::Included(start), Bound::Unbounded)),
                    }
                }

                fn persist(&self, store: &S) -> Result<(), Self::Error> {
                    use ::bobsled::EncodeKey;

                    store.insert((self.id, self.key.clone()).encode().as_ref(), &[]).map_err(|err| Self::Error::StoreError(err))
                }
            }
        };
    };
    tokens.into()
}
*/
