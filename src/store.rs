use std::{collections::BTreeMap, ops::RangeBounds, sync::Mutex, vec};

pub trait Store {
    type Error: std::error::Error;
    type Data: AsRef<[u8]>;
    type Iter: Iterator<Item = Result<(Self::Data, Self::Data), Self::Error>>;

    fn fetch(&self, key: &[u8]) -> Result<Option<Self::Data>, Self::Error>;
    fn range(&self, range: impl RangeBounds<[u8]>) -> Self::Iter;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;
}

impl Store for Mutex<BTreeMap<Vec<u8>, Vec<u8>>> {
    type Error = std::convert::Infallible;
    type Data = Vec<u8>;
    type Iter = vec::IntoIter<Result<(Self::Data, Self::Data), Self::Error>>;

    fn fetch(&self, key: &[u8]) -> Result<Option<Self::Data>, Self::Error> {
        let guard = self.lock().unwrap();

        Ok(guard.get(key).map(Clone::clone))
    }

    fn range(&self, range: impl RangeBounds<[u8]>) -> Self::Iter {
        match self.lock() {
            Ok(guard) => guard
                .range(range)
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect::<Vec<_>>()
                .into_iter(),
            Err(_) => todo!(),
        }
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut guard = self.lock().unwrap();

        guard.insert(key.to_owned(), value.to_owned());

        Ok(())
    }
}

#[cfg(feature = "sled")]
const _: () = {
    impl Store for sled::Tree {
        type Error = sled::Error;
        type Data = sled::IVec;
        type Iter = sled::Iter;

        #[inline]
        fn fetch(&self, key: &[u8]) -> Result<Option<Self::Data>, Self::Error> {
            self.get(key)
        }

        #[inline]
        fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.insert(key, value).and(Ok(()))
        }

        fn range(&self, range: impl RangeBounds<[u8]>) -> Self::Iter {
            let start = range.start_bound();
            let end = range.end_bound();

            self.range::<&[u8], _>((start, end))
        }
    }

    impl Store for sled::transaction::TransactionalTree {
        type Error = sled::transaction::UnabortableTransactionError;
        type Data = sled::IVec;
        type Iter = std::iter::Empty<Result<(sled::IVec, sled::IVec), Self::Error>>;

        #[inline]
        fn fetch(&self, key: &[u8]) -> Result<Option<Self::Data>, Self::Error> {
            self.get(key)
        }

        #[inline]
        fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.insert(key, value).and(Ok(()))
        }

        fn range(&self, _range: impl RangeBounds<[u8]>) -> Self::Iter {
            unimplemented!("Sled transactions do not support scan operations")
        }
    }
};

