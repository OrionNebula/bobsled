use std::{
    convert::Infallible,
    ffi::{OsStr, OsString},
    ops::Deref,
    path::{Path, PathBuf},
    str::Utf8Error,
};

use crate::{DecodeKey, EncodeKey, PrefixKey};

/// A special key wrapper that encodes "greedily" (that is, consumes all remaining space when encoding/decoding)
/// Useful for performing simple string prefix searches
#[derive(Debug, Clone, Copy)]
pub struct GreedyKey<T>(pub T)
where
    Self: EncodeKey;

impl<T> From<T> for GreedyKey<T>
where
    Self: EncodeKey,
{
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for GreedyKey<T>
where
    Self: EncodeKey,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl EncodeKey for GreedyKey<Vec<u8>> {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.0.clone()
    }
}

impl DecodeKey for GreedyKey<Vec<u8>> {
    type Error = Infallible;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        Ok((GreedyKey(bytes.into()), &[]))
    }
}

impl EncodeKey for GreedyKey<String> {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        self.0.as_bytes().into()
    }
}

impl DecodeKey for GreedyKey<String> {
    type Error = Utf8Error;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        Ok((GreedyKey(std::str::from_utf8(bytes)?.into()), &[]))
    }
}

impl<'a> EncodeKey for GreedyKey<&'a str> {
    type Bytes = &'a [u8];

    fn encode(&self) -> Self::Bytes {
        self.0.as_bytes()
    }
}

impl<'a> PrefixKey<GreedyKey<String>> for GreedyKey<&'a str> {}

impl EncodeKey for GreedyKey<OsString> {
    type Bytes = Vec<u8>;

    #[cfg(unix)]
    fn encode(&self) -> Self::Bytes {
        use std::os::unix::ffi::OsStrExt;

        self.as_os_str().as_bytes().to_owned()
    }
}

#[cfg(unix)]
impl DecodeKey for GreedyKey<OsString> {
    type Error = Infallible;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        use std::os::unix::ffi::OsStringExt;

        Ok((GreedyKey(OsString::from_vec(bytes.into())), &[]))
    }
}

#[cfg(unix)]
impl<'a> EncodeKey for GreedyKey<&'a OsStr> {
    type Bytes = &'a [u8];

    fn encode(&self) -> Self::Bytes {
        use std::os::unix::ffi::OsStrExt;

        self.0.as_bytes()
    }
}

impl<'a> PrefixKey<GreedyKey<OsString>> for GreedyKey<&'a OsStr> {}

impl EncodeKey for GreedyKey<PathBuf> {
    type Bytes = Vec<u8>;

    fn encode(&self) -> Self::Bytes {
        GreedyKey(self.0.as_os_str()).encode().into()
    }
}

impl DecodeKey for GreedyKey<PathBuf> {
    type Error = Infallible;

    fn try_decode(bytes: &[u8]) -> Result<(Self, &[u8]), Self::Error> {
        GreedyKey::<OsString>::try_decode(bytes)
            .map(|(GreedyKey(os_string), suffix)| (GreedyKey(PathBuf::from(os_string)), suffix))
    }
}

impl<'a> EncodeKey for GreedyKey<&'a Path> {
    type Bytes = <GreedyKey<&'a str> as EncodeKey>::Bytes;

    fn encode(&self) -> Self::Bytes {
        GreedyKey(self.as_os_str()).encode()
    }
}

impl<'a> PrefixKey<GreedyKey<PathBuf>> for GreedyKey<&'a Path> {}
