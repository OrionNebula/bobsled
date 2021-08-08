use std::{collections::BTreeMap, convert::Infallible, str::Utf8Error};

use bobsled::Record;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestRecord {
    pub id: u64,
    pub data: String,
}

impl Record for TestRecord {
    type Key = u64;
    type EncodeError = Infallible;
    type DecodeError = Utf8Error;

    fn try_encode(&self) -> Result<(Self::Key, Vec<u8>), Self::EncodeError> {
        Ok((self.id, self.data.as_bytes().into()))
    }

    fn try_decode(key: Self::Key, value: &[u8]) -> Result<Self, Self::DecodeError> {
        Ok(Self {
            id: key,
            data: std::str::from_utf8(value)?.into(),
        })
    }
}

#[test]
fn test() {
    let mut store = BTreeMap::new();

    let record = TestRecord {
        id: 0,
        data: "Hello there!".into(),
    };
    record.persist(&mut store).unwrap();

    assert_eq!(Ok(Some(record.clone())), TestRecord::fetch(&store, &0));

    let records = TestRecord::scan(&store)
        .collect::<Result<Vec<_>, _>>()
        .expect("Iterating should succeed");

    assert_eq!(records.len(), 1);
    assert_eq!(Some(record), records.into_iter().next());

    let record = TestRecord {
        id: 1,
        data: "Hello there!".into(),
    };
    record.persist(&mut store).unwrap();

    let n_records = TestRecord::scan_range(&store, 0u64..=1u64).count();
    assert_eq!(n_records, 2);

    let n_records = TestRecord::scan_range(&store, 0u64..1u64).count();
    assert_eq!(n_records, 1);
}
