use std::{
    convert::{Infallible, TryInto},
    ffi::{CStr, CString, FromBytesWithNulError, OsStr, OsString},
    fmt::{Debug, Display},
    path::{Path, PathBuf},
    rc::Rc,
    str::Utf8Error,
    sync::Arc,
};

use crate::DataTooShort;

pub trait EncodeKey {
    type Bytes: AsRef<[u8]>;

    fn encode(&self) -> Self::Bytes;
}

/// Implement encode for one or more container types
macro_rules! impl_container_encode {
    ($($type:ty),+) => {$(
        impl<T> EncodeKey for $type where T: EncodeKey {
            type Bytes = T::Bytes;

            fn encode(&self) -> Self::Bytes {
                T::encode(self.as_ref())
            }
        }
    )+};
}

impl_container_encode!(Box<T>, Arc<T>, Rc<T>);

impl EncodeKey for () {
    type Bytes = [u8; 0];

    fn encode(&self) -> Self::Bytes {
        []
    }
}

impl EncodeKey for String {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.as_str().encode()
    }
}

impl EncodeKey for str {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        let bytes = self.as_bytes();
        let mut vec = Vec::with_capacity(std::mem::size_of::<usize>() + bytes.len());

        vec.extend_from_slice(&bytes.len().to_be_bytes());
        vec.extend_from_slice(bytes);

        vec
    }
}

impl<'a> EncodeKey for &'a str {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        str::encode(*self)
    }
}

/// Encoded as a nul-terminated string. Useful for prefix matching, but can't contain interior null bytes
impl EncodeKey for CString {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.as_bytes_with_nul().to_owned()
    }
}

impl EncodeKey for CStr {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.to_bytes_with_nul().to_owned()
    }
}

impl<T> EncodeKey for Vec<T>
where
    T: EncodeKey,
{
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        AsRef::<[T]>::as_ref(self).encode()
    }
}

impl<T> EncodeKey for [T]
where
    T: EncodeKey,
{
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        let mut vec = Vec::new();

        vec.extend_from_slice(&self.len().to_be_bytes());
        for item in self {
            vec.extend_from_slice(item.encode().as_ref());
        }

        vec
    }
}

impl<const N: usize> EncodeKey for [u8; N] {
    type Bytes = [u8; N];

    fn encode(&self) -> Self::Bytes {
        *self
    }
}

impl EncodeKey for OsString {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.as_os_str().encode()
    }
}

impl EncodeKey for OsStr {
    type Bytes = Vec<u8>;

    #[cfg(unix)]
    fn encode(&self) -> Self::Bytes {
        use std::os::unix::ffi::OsStrExt;

        self.as_bytes().encode()
    }

    #[cfg(windows)]
    fn encode(&self) -> Self::Bytes {
        use std::os::windows::ffi::OsStrExt;

        self.encode_wide().collect::<Vec<u16>>().encode()
    }
}

impl EncodeKey for PathBuf {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.as_path().encode()
    }
}

impl EncodeKey for Path {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.as_os_str().encode()
    }
}

pub trait DecodeKey: Sized + EncodeKey {
    type Error: std::error::Error;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error>;
}

impl DecodeKey for () {
    type Error = Infallible;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        Ok(((), bytes))
    }
}

#[derive(Debug)]
pub enum StringDecodeError {
    Utf8Error(Utf8Error),
    DataTooShort(DataTooShort),
}

impl From<Utf8Error> for StringDecodeError {
    fn from(err: Utf8Error) -> Self {
        Self::Utf8Error(err)
    }
}

impl From<DataTooShort> for StringDecodeError {
    fn from(err: DataTooShort) -> Self {
        Self::DataTooShort(err)
    }
}

impl Display for StringDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8Error(err) => Display::fmt(err, f),
            Self::DataTooShort(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for StringDecodeError {}

impl DecodeKey for String {
    type Error = StringDecodeError;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        if bytes.len() < std::mem::size_of::<usize>() {
            return Err(StringDecodeError::DataTooShort(DataTooShort {
                expected: std::mem::size_of::<usize>(),
                actual: bytes.len(),
            }));
        }

        let (len, bytes) = bytes.split_at(std::mem::size_of::<usize>());
        let len = usize::from_be_bytes(len.try_into().unwrap());

        if bytes.len() < len {
            return Err(StringDecodeError::DataTooShort(DataTooShort {
                expected: len,
                actual: bytes.len(),
            }));
        }
        let (data, bytes) = bytes.split_at(len);

        Ok((std::str::from_utf8(data)?.into(), bytes))
    }
}

impl DecodeKey for CString {
    type Error = FromBytesWithNulError;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        let null_pos = bytes
            .iter()
            .position(|b| *b == 0x00)
            .map(|pos| pos + 1)
            .unwrap_or(bytes.len());

        let (data, bytes) = bytes.split_at(null_pos);
        CStr::from_bytes_with_nul(data).map(move |cstr| (cstr.into(), bytes))
    }
}

#[cfg(unix)]
impl DecodeKey for OsString {
    type Error = VecDecodeError<DataTooShort>;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        use std::os::unix::ffi::OsStringExt;

        Vec::<u8>::try_decode(bytes).map(|(vec, bytes)| (OsString::from_vec(vec), bytes))
    }
}

#[derive(Debug)]
pub enum VecDecodeError<E: std::error::Error> {
    HeaderError(DataTooShort),
    ElementError(E),
}

impl<E: std::error::Error> Display for VecDecodeError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeaderError(err) => Display::fmt(err, f),
            Self::ElementError(err) => Display::fmt(err, f),
        }
    }
}

impl<E: std::error::Error> std::error::Error for VecDecodeError<E> {}

impl<T: DecodeKey> DecodeKey for Vec<T> {
    type Error = VecDecodeError<T::Error>;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        if bytes.len() < std::mem::size_of::<usize>() {
            return Err(VecDecodeError::HeaderError(DataTooShort {
                expected: std::mem::size_of::<usize>(),
                actual: bytes.len(),
            }));
        }

        let (len, mut bytes) = bytes.split_at(std::mem::size_of::<usize>());
        let len = usize::from_be_bytes(len.try_into().unwrap());

        let mut buf = Vec::with_capacity(len);
        while buf.len() < len {
            match T::try_decode(bytes) {
                Ok((elem, suffix)) => {
                    buf.push(elem);

                    bytes = suffix;
                }
                Err(err) => return Err(VecDecodeError::ElementError(err)),
            }
        }

        Ok((buf, bytes))
    }
}

impl<const N: usize> DecodeKey for [u8; N] {
    type Error = DataTooShort;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        if bytes.len() < N {
            return Err(DataTooShort {
                expected: N,
                actual: bytes.len(),
            });
        }

        let (data, bytes) = bytes.split_at(N);
        Ok((data.try_into().unwrap(), bytes))
    }
}

/// Denotes that Self is a valid prefix of K
pub trait PrefixKey<K: EncodeKey>: EncodeKey {}

impl<K> PrefixKey<K> for K where K: EncodeKey {}

macro_rules! impl_uint_key {
    ($($type:ty),+) => {$(
        impl $crate::EncodeKey for $type {
            type Bytes = [u8; ::std::mem::size_of::<$type>()];

            fn encode(&self) -> Self::Bytes {
                self.to_be_bytes()
            }
        }

        impl $crate::DecodeKey for $type {
            type Error = $crate::DataTooShort;

            fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
                if bytes.len() < ::std::mem::size_of::<$type>() {
                    return Err($crate::DataTooShort { expected: ::std::mem::size_of::<$type>(), actual: bytes.len() });
                }

                let (data, bytes) = bytes.split_at(::std::mem::size_of::<$type>());

                Ok((Self::from_be_bytes(::std::convert::TryInto::try_into(data).unwrap()), bytes))
            }
        }
    )+};
}

macro_rules! impl_iint_key {
    ($($type:ty),+) => {$(
        impl $crate::EncodeKey for $type {
            type Bytes = [u8; ::std::mem::size_of::<$type>()];

            fn encode(&self) -> Self::Bytes {
                let mut bytes = self.to_be_bytes();
                bytes[0] ^= 0x80;

                bytes
            }
        }

        impl $crate::DecodeKey for $type {
            type Error = $crate::DataTooShort;

            fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
                if bytes.len() < ::std::mem::size_of::<$type>() {
                    return Err($crate::DataTooShort { expected: ::std::mem::size_of::<$type>(), actual: bytes.len() });
                }

                let (data, bytes) = bytes.split_at(::std::mem::size_of::<$type>());
                let mut data: [u8; ::std::mem::size_of::<$type>()] = ::std::convert::TryInto::try_into(data).unwrap();
                data[0] ^= 0x80;

                Ok((Self::from_be_bytes(data), bytes))
            }
        }
    )+};
}

/// See https://github.com/chitin-io/web/issues/2
macro_rules! impl_float_key {
    ($($type:ty),+) => {$(
        impl $crate::EncodeKey for $type {
            type Bytes = [u8; ::std::mem::size_of::<$type>()];

            fn encode(&self) -> Self::Bytes {
                let mut bytes = self.to_be_bytes();

                if bytes[0] & 0x80 == 0x00 {
                    bytes[0] ^= 0x80
                } else {
                    for b in bytes.iter_mut() {
                        *b ^= 0xFF;
                    }
                }

                bytes
            }
        }

        impl $crate::DecodeKey for $type {
            type Error = $crate::DataTooShort;

            fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
                if bytes.len() < ::std::mem::size_of::<$type>() {
                    return Err($crate::DataTooShort { expected: ::std::mem::size_of::<$type>(), actual: bytes.len() });
                }

                let (data, bytes) = bytes.split_at(::std::mem::size_of::<$type>());
                let mut data: [u8; ::std::mem::size_of::<$type>()] = ::std::convert::TryInto::try_into(data).unwrap();

                if data[0] & 0x80 == 0x80 {
                    data[0] ^= 0x80;
                } else {
                    for b in data.iter_mut() {
                        *b ^= 0xFF;
                    }
                }

                Ok((Self::from_be_bytes(data), bytes))
            }
        }
    )+};
}

impl_uint_key!(u8, u16, u32, u64, usize);
impl_iint_key!(i8, i16, i32, i64, isize);
impl_float_key!(f32, f64);

macro_rules! impl_tuple_key {
    ($($gen:ident),+) => {
        impl<$($gen,)+> $crate::EncodeKey for ($($gen),+,) where $($gen: $crate::EncodeKey),+ {
            type Bytes = Vec<u8>;

            fn encode(&self) -> Self::Bytes {
                let mut vec = Vec::new();

                #[allow(non_snake_case)]
                let ($($gen),+,) = self;

                $(
                    vec.extend_from_slice($gen.encode().as_ref());
                )+

                vec
            }
        }

        ::bobsled_macros::impl_tuple_prefix!(($($gen),+,));

        impl<$($gen,)+> $crate::PrefixKey<($($gen),+,)> for A where $($gen: $crate::EncodeKey),+ {}

        const _: () = {
            paste! {
                #[derive(Debug)]
                pub enum [< TupleDecodeError $($gen)+ >]<$($gen,)+> where $($gen: ::std::error::Error),+ {
                    $(
                        [< Decode $gen Error >]($gen)
                    ),+
                }

                impl<$($gen,)+> ::std::fmt::Display for [< TupleDecodeError $($gen)+ >]<$($gen),+> where $($gen: ::std::error::Error),+ {
                    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                        match self {
                            $(
                                Self::[< Decode $gen Error >](err) => ::std::fmt::Display::fmt(err, f)
                            ),+
                        }
                    }
                }

                impl<$($gen,)+> ::std::error::Error for [< TupleDecodeError $($gen)+ >]<$($gen),+> where $($gen: ::std::error::Error),+ { }
            }

            impl<$($gen,)+> $crate::DecodeKey for ($($gen),+,) where Self: Sized, $($gen: $crate::DecodeKey),+ {
                type Error = paste! { [< TupleDecodeError $($gen)+ >]::<$($gen::Error),+> };

                fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
                    $(
                        #[allow(non_snake_case)]
                        let ($gen, bytes) = match $gen::try_decode(bytes) {
                            Ok(x) => x,
                            Err(err) => paste! { return Err(Self::Error::[< Decode $gen Error >](err)) }
                        };
                    )+

                    Ok((
                        (
                            $($gen),+,
                        ),
                        bytes
                    ))
                }
            }
        };
    };
}

// Implement the EncodeKey, DecodeKey, and PrefixKey traits for N-tuples up to 26
impl_tuple_key!(A);
impl_tuple_key!(A, B);
impl_tuple_key!(A, B, C);
impl_tuple_key!(A, B, C, D);
impl_tuple_key!(A, B, C, D, E);
impl_tuple_key!(A, B, C, D, E, F);
impl_tuple_key!(A, B, C, D, E, F, G);
impl_tuple_key!(A, B, C, D, E, F, G, H);
impl_tuple_key!(A, B, C, D, E, F, G, H, I);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y);
impl_tuple_key!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z);

#[cfg(feature = "greedy")]
mod greedy;
#[cfg(feature = "greedy")]
pub use greedy::*;

#[cfg(feature = "widestring")]
mod widestring;
