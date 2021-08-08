#![forbid(unsafe_code)]

use std::{fmt::Display, ops::RangeBounds};

#[macro_use]
extern crate paste;

// pub use bobsled_macros::Record;

mod key;
pub use key::*;

mod store;
pub use store::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataTooShort {
    pub expected: usize,
    pub actual: usize,
}

impl Display for DataTooShort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected data to be at least {} bytes long, was actually only {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for DataTooShort {}

pub trait Record: Sized {
    type Key: EncodeKey + DecodeKey + Clone;
    type EncodeError: std::error::Error;
    type DecodeError: std::error::Error;

    fn try_encode(&self) -> Result<(Self::Key, Vec<u8>), Self::EncodeError>;
    fn try_decode(key: Self::Key, value: &[u8]) -> Result<Self, Self::DecodeError>;

    #[inline]
    fn fetch<S: ReadStore<Self>>(
        store: S,
        key: &Self::Key,
    ) -> Result<
        Option<Self>,
        ReadStoreError<S::Error, <Self::Key as DecodeKey>::Error, Self::DecodeError>,
    > {
        store.fetch(key)
    }

    #[inline]
    fn scan<S: ReadStore<Self>>(store: S) -> S::Iter {
        store.scan()
    }

    #[inline]
    fn scan_range<S: ReadStore<Self>, P: PrefixKey<Self::Key>>(
        store: S,
        range: impl RangeBounds<P>,
    ) -> S::Iter {
        store.scan_range(range)
    }

    #[inline]
    fn scan_prefix<S: ReadStore<Self>, P: PrefixKey<Self::Key>>(store: S, prefix: &P) -> S::Iter {
        store.scan_prefix(prefix)
    }

    #[inline]
    fn persist<S: WriteStore<Self>>(
        &self,
        store: S,
    ) -> Result<(), WriteStoreError<S::Error, Self::EncodeError>> {
        store.persist(self)
    }

    #[inline]
    fn remove<S: WriteStore<Self>>(
        store: S,
        key: &Self::Key,
    ) -> Result<(), S::Error> {
        store.remove(key)
    }
}
