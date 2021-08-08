use std::{
    collections::{btree_map, BTreeMap},
    convert::Infallible,
    fmt::Display,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{DecodeKey, EncodeKey, PrefixKey, Record};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStoreError<S: std::error::Error, K: std::error::Error, V: std::error::Error> {
    StoreError(S),
    KeyDecodeErr(K),
    ValueDecodeError(V),
}

impl<S: std::error::Error, K: std::error::Error, V: std::error::Error> Display
    for ReadStoreError<S, K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StoreError(err) => Display::fmt(err, f),
            Self::KeyDecodeErr(err) => Display::fmt(err, f),
            Self::ValueDecodeError(err) => Display::fmt(err, f),
        }
    }
}

impl<S: std::error::Error, K: std::error::Error, V: std::error::Error> std::error::Error
    for ReadStoreError<S, K, V>
{
}

pub trait ReadStore<R: Record> {
    type Error: std::error::Error;
    type Iter: Iterator<
        Item = Result<R, ReadStoreError<Self::Error, <R::Key as DecodeKey>::Error, R::DecodeError>>,
    >;

    fn fetch(
        self,
        key: &R::Key,
    ) -> Result<Option<R>, ReadStoreError<Self::Error, <R::Key as DecodeKey>::Error, R::DecodeError>>;

    fn scan(self) -> Self::Iter;
    fn scan_range<P: PrefixKey<R::Key>>(self, range: impl RangeBounds<P>) -> Self::Iter;
    fn scan_prefix<P: PrefixKey<R::Key>>(self, prefix: &P) -> Self::Iter;
}

pub struct BTreeStoreIter<'a, R: Record> {
    iter: btree_map::Range<'a, Vec<u8>, Vec<u8>>,
    _phantom: PhantomData<R>,
}

impl<'a, R: Record> Iterator for BTreeStoreIter<'a, R> {
    type Item = Result<R, ReadStoreError<Infallible, <R::Key as DecodeKey>::Error, R::DecodeError>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some((key, value)) => {
                let key = match <R::Key as DecodeKey>::try_decode(key.as_ref()) {
                    Ok((key, _)) => key,
                    Err(err) => return Some(Err(ReadStoreError::KeyDecodeErr(err))),
                };

                match R::try_decode(key, value.as_ref()) {
                    Ok(record) => Some(Ok(record)),
                    Err(err) => Some(Err(ReadStoreError::ValueDecodeError(err))),
                }
            }
            None => None,
        }
    }
}

impl<'a, R: Record> ReadStore<R> for &'a BTreeMap<Vec<u8>, Vec<u8>> {
    type Error = Infallible;
    type Iter = BTreeStoreIter<'a, R>;

    fn fetch(
        self,
        key: &R::Key,
    ) -> Result<Option<R>, ReadStoreError<Self::Error, <R::Key as DecodeKey>::Error, R::DecodeError>>
    {
        let key_data = key.encode();

        let value = match self.get(key_data.as_ref()) {
            Some(value) => value,
            None => return Ok(None),
        };

        match R::try_decode(key.clone(), value.as_ref()) {
            Ok(record) => Ok(Some(record)),
            Err(err) => Err(ReadStoreError::ValueDecodeError(err)),
        }
    }

    fn scan(self) -> Self::Iter {
        BTreeStoreIter {
            iter: self.range::<[u8], _>((Bound::Unbounded, Bound::Unbounded)),
            _phantom: PhantomData,
        }
    }

    fn scan_range<P: PrefixKey<R::Key>>(self, range: impl RangeBounds<P>) -> Self::Iter {
        let start = match range.start_bound() {
            Bound::Excluded(start) => Bound::Excluded(start.encode()),
            Bound::Included(start) => Bound::Included(start.encode()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let start = match start {
            Bound::Excluded(ref start) => Bound::Excluded(start.as_ref()),
            Bound::Included(ref start) => Bound::Included(start.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };

        match range.end_bound() {
            Bound::Excluded(end) => {
                let end = end.encode();
                let end = Bound::Excluded(end.as_ref());

                BTreeStoreIter {
                    iter: self.range::<[u8], _>((start, end)),
                    _phantom: PhantomData,
                }
            }
            Bound::Included(end) => {
                let end = end.encode();
                let mut end = end.as_ref().to_owned();

                while let Some(last) = end.pop() {
                    if last < u8::MAX {
                        end.push(last + 1);
                        return BTreeStoreIter {
                            iter: self.range::<[u8], _>((start, Bound::Excluded(end.as_ref()))),
                            _phantom: PhantomData,
                        };
                    }
                }

                BTreeStoreIter {
                    iter: self.range::<[u8], _>((start, Bound::Unbounded)),
                    _phantom: PhantomData,
                }
            }
            Bound::Unbounded => BTreeStoreIter {
                iter: self.range::<[u8], _>((start, Bound::Unbounded)),
                _phantom: PhantomData,
            },
        }
    }

    fn scan_prefix<P: PrefixKey<R::Key>>(self, prefix: &P) -> Self::Iter {
        let prefix = prefix.encode();
        let start = prefix.as_ref();

        let mut end = start.to_owned();
        while let Some(last) = end.pop() {
            if last < u8::MAX {
                end.push(last + 1);
                return BTreeStoreIter {
                    iter: self
                        .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end.as_ref()))),
                    _phantom: PhantomData,
                };
            }
        }

        BTreeStoreIter {
            iter: self.range::<[u8], _>((Bound::Included(start), Bound::Unbounded)),
            _phantom: PhantomData,
        }
    }
}

pub trait WriteStore<R: Record> {
    type Error: std::error::Error;

    fn persist(self, record: &R) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>>;
    fn remove(self, key: &R::Key) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStoreError<S: std::error::Error, V: std::error::Error> {
    StoreError(S),
    EncodeError(V),
}

impl<S: std::error::Error, V: std::error::Error> Display for WriteStoreError<S, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteStoreError::StoreError(err) => Display::fmt(err, f),
            WriteStoreError::EncodeError(err) => Display::fmt(err, f),
        }
    }
}

impl<'a, R: Record> WriteStore<R> for &'a mut BTreeMap<Vec<u8>, Vec<u8>> {
    type Error = Infallible;

    fn persist(self, record: &R) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
        let (key, value) = match record.try_encode() {
            Ok(data) => data,
            Err(err) => return Err(WriteStoreError::EncodeError(err)),
        };

        self.insert(key.encode().as_ref().into(), value);

        Ok(())
    }

    fn remove(self, key: &R::Key) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
        self.remove(key.encode().as_ref());

        Ok(())
    }
}

#[cfg(feature = "sled")]
const _: () = {
    pub struct SledIter<R: Record> {
        iter: sled::Iter,
        _phantom: PhantomData<R>,
    }

    impl<R: Record> Iterator for SledIter<R> {
        type Item =
            Result<R, ReadStoreError<sled::Error, <R::Key as DecodeKey>::Error, R::DecodeError>>;

        fn next(&mut self) -> Option<Self::Item> {
            match self.iter.next() {
                Some(Ok((key, value))) => {
                    let key = match <R::Key as DecodeKey>::try_decode(key.as_ref()) {
                        Ok((key, _)) => key,
                        Err(err) => return Some(Err(ReadStoreError::KeyDecodeErr(err))),
                    };

                    match R::try_decode(key, value.as_ref()) {
                        Ok(record) => Some(Ok(record)),
                        Err(err) => Some(Err(ReadStoreError::ValueDecodeError(err))),
                    }
                }
                Some(Err(err)) => Some(Err(ReadStoreError::StoreError(err))),
                None => None,
            }
        }
    }

    impl<'a, R: Record> ReadStore<R> for &'a sled::Tree {
        type Error = sled::Error;
        type Iter = SledIter<R>;

        fn fetch(
            self,
            key: &R::Key,
        ) -> Result<
            Option<R>,
            ReadStoreError<Self::Error, <R::Key as DecodeKey>::Error, R::DecodeError>,
        > {
            let key_data = key.encode();

            let value = match self.get(key_data) {
                Ok(Some(value)) => value,
                Ok(None) => return Ok(None),
                Err(err) => return Err(ReadStoreError::StoreError(err)),
            };

            match R::try_decode(key.clone(), value.as_ref()) {
                Ok(record) => Ok(Some(record)),
                Err(err) => Err(ReadStoreError::ValueDecodeError(err)),
            }
        }

        fn scan(self) -> Self::Iter {
            SledIter {
                iter: self.iter(),
                _phantom: PhantomData,
            }
        }

        fn scan_range<P: PrefixKey<R::Key>>(self, range: impl RangeBounds<P>) -> Self::Iter {
            let start = match range.start_bound() {
                Bound::Excluded(start) => Bound::Excluded(start.encode()),
                Bound::Included(start) => Bound::Included(start.encode()),
                Bound::Unbounded => Bound::Unbounded,
            };

            let start = match start {
                Bound::Excluded(ref start) => Bound::Excluded(start.as_ref()),
                Bound::Included(ref start) => Bound::Included(start.as_ref()),
                Bound::Unbounded => Bound::Unbounded,
            };

            match range.end_bound() {
                Bound::Excluded(end) => {
                    let end = end.encode();
                    let end = Bound::Excluded(end.as_ref());

                    SledIter {
                        iter: self.range::<&[u8], _>((start, end)),
                        _phantom: PhantomData,
                    }
                }
                Bound::Included(end) => {
                    let end = end.encode();
                    let mut end = end.as_ref().to_owned();

                    while let Some(last) = end.pop() {
                        if last < u8::MAX {
                            end.push(last + 1);
                            return SledIter {
                                iter: self
                                    .range::<&[u8], _>((start, Bound::Excluded(end.as_ref()))),
                                _phantom: PhantomData,
                            };
                        }
                    }

                    SledIter {
                        iter: self.range::<&[u8], _>((start, Bound::Unbounded)),
                        _phantom: PhantomData,
                    }
                }
                Bound::Unbounded => SledIter {
                    iter: self.range::<&[u8], _>((start, Bound::Unbounded)),
                    _phantom: PhantomData,
                },
            }
        }

        fn scan_prefix<P: PrefixKey<R::Key>>(self, prefix: &P) -> Self::Iter {
            SledIter {
                iter: self.scan_prefix(prefix.encode().as_ref()),
                _phantom: PhantomData,
            }
        }
    }

    impl<'a, R: Record> WriteStore<R> for &'a sled::Tree {
        type Error = sled::Error;

        fn persist(self, record: &R) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
            let (key, value) = match record.try_encode() {
                Ok(data) => data,
                Err(err) => return Err(WriteStoreError::EncodeError(err)),
            };

            match self.insert(key.encode().as_ref(), value) {
                Ok(_) => Ok(()),
                Err(err) => Err(WriteStoreError::StoreError(err)),
            }
        }

        fn remove(self, key: &R::Key) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
            match self.remove(key.encode().as_ref()) {
                Ok(_) => Ok(()),
                Err(err) => Err(WriteStoreError::StoreError(err)),
            }
        }
    }

    impl<'a, R: Record> ReadStore<R> for &'a sled::transaction::TransactionalTree {
        type Error = sled::transaction::UnabortableTransactionError;
        type Iter = std::iter::Empty<
            Result<R, ReadStoreError<Self::Error, <R::Key as DecodeKey>::Error, R::DecodeError>>,
        >;

        fn fetch(
            self,
            key: &R::Key,
        ) -> Result<
            Option<R>,
            ReadStoreError<Self::Error, <R::Key as DecodeKey>::Error, R::DecodeError>,
        > {
            let key_data = key.encode();

            let value = match self.get(key_data) {
                Ok(Some(value)) => value,
                Ok(None) => return Ok(None),
                Err(err) => return Err(ReadStoreError::StoreError(err)),
            };

            match R::try_decode(key.clone(), value.as_ref()) {
                Ok(record) => Ok(Some(record)),
                Err(err) => Err(ReadStoreError::ValueDecodeError(err)),
            }
        }

        fn scan(self) -> Self::Iter {
            unimplemented!("Sled transactions do not support scan operations")
        }

        fn scan_range<P: PrefixKey<R::Key>>(self, _range: impl RangeBounds<P>) -> Self::Iter {
            unimplemented!("Sled transactions do not support scan operations")
        }

        fn scan_prefix<P: PrefixKey<R::Key>>(self, _prefix: &P) -> Self::Iter {
            unimplemented!("Sled transactions do not support scan operations")
        }
    }

    impl<'a, R: Record> WriteStore<R> for &'a sled::transaction::TransactionalTree {
        type Error = sled::transaction::UnabortableTransactionError;

        fn persist(self, record: &R) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
            let (key, value) = match record.try_encode() {
                Ok(data) => data,
                Err(err) => return Err(WriteStoreError::EncodeError(err)),
            };

            match self.insert(key.encode().as_ref(), value) {
                Ok(_) => Ok(()),
                Err(err) => Err(WriteStoreError::StoreError(err)),
            }
        }

        fn remove(self, key: &R::Key) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
            match self.remove(key.encode().as_ref()) {
                Ok(_) => Ok(()),
                Err(err) => Err(WriteStoreError::StoreError(err)),
            }
        }
    }

    impl<'a, R: Record> WriteStore<R> for &'a mut sled::Batch {
        type Error = Infallible;

        fn persist(self, record: &R) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
            let (key, value) = match record.try_encode() {
                Ok(data) => data,
                Err(err) => return Err(WriteStoreError::EncodeError(err)),
            };

            self.insert(key.encode().as_ref(), value);

            Ok(())
        }

        fn remove(self, key: &R::Key) -> Result<(), WriteStoreError<Self::Error, R::EncodeError>> {
            self.remove(key.encode().as_ref());

            Ok(())
        }
    }
};
