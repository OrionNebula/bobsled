use std::{
    fmt::Display,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

#[macro_use]
extern crate paste;

// pub use bobsled_macros::Record;

mod key;
pub use key::*;

mod store;
pub use store::*;

#[derive(Debug)]
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

pub struct RecordIter<S: Store, R: Record<S>> {
    iter: S::Iter,
    _phantom: PhantomData<R>,
}

impl<S: Store, R: Record<S>> Iterator for RecordIter<S, R> {
    type Item = Result<R, R::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next()? {
            Ok((key, value)) => match R::Key::try_decode(key.as_ref()) {
                Ok((key, _)) => Some(R::try_decode(key, value.as_ref())),
                Err(err) => Some(Err(err.into())),
            },
            Err(err) => Some(Err(err.into())),
        }
    }
}

pub trait Record<S>: Sized
where
    S: Store,
{
    type Key: EncodeKey + DecodeKey + Clone;
    type Error: From<S::Error> + From<<Self::Key as DecodeKey>::Error>;

    fn try_decode(key: Self::Key, value: &[u8]) -> Result<Self, Self::Error>;
    fn try_encode(&self) -> Result<(Self::Key, Vec<u8>), Self::Error>;

    fn fetch(store: &S, key: &Self::Key) -> Result<Option<Self>, Self::Error> {
        match store.fetch(key.encode().as_ref()) {
            Ok(Some(value)) => Ok(Some(Self::try_decode(key.clone(), value.as_ref())?)),
            Ok(None) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn scan(store: &S) -> RecordIter<S, Self> {
        RecordIter {
            iter: store.range(..),
            _phantom: PhantomData,
        }
    }

    fn scan_range<K: PrefixKey<Self::Key>>(
        store: &S,
        range: impl RangeBounds<K>,
    ) -> RecordIter<S, Self> {
        let start = range.start_bound();
        let end = range.end_bound();

        // TODO: We need to transform an inclusive bound into an exclusive using the same prefix-y logic as below
        // The reason for this is to make querying more ergonimic, i.e. an inclusive numeric bound will actually behave as expected

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

        RecordIter {
            iter: store.range((start, end)),
            _phantom: PhantomData,
        }
    }

    fn scan_prefix(store: &S, prefix: &impl PrefixKey<Self::Key>) -> RecordIter<S, Self> {
        // Perform a prefix scan by constructing a range based on the prefix key and scanning that
        // Requires an allocation (but tbf so does sled)

        let prefix = prefix.encode();
        let start = prefix.as_ref();

        let mut end = start.to_owned();
        while let Some(last) = end.pop() {
            if last < u8::MAX {
                end.push(last + 1);
                return RecordIter {
                    iter: store.range((Bound::Included(start), Bound::Included(end.as_ref()))),
                    _phantom: PhantomData,
                };
            }
        }

        RecordIter {
            iter: store.range((Bound::Included(start), Bound::Unbounded)),
            _phantom: PhantomData,
        }
    }

    fn persist(&self, store: &S) -> Result<(), Self::Error> {
        let (key, value) = self.try_encode()?;

        store
            .insert(key.encode().as_ref(), value.as_ref())
            .map_err(Into::into)
    }
}
