use crate::GreedyKey;

use super::EncodeKey;
use widestring::{UStr, UString};

macro_rules! impl_widestr {
    ($($uchar:ty),+) => {$(
        impl EncodeKey for UStr<$uchar> {
            type Bytes = Vec<u8>;

            fn encode(&self) -> Self::Bytes {
                self.as_slice().encode()
            }
        }

        impl EncodeKey for UString<$uchar> {
            type Bytes = Vec<u8>;

            fn encode(&self) -> Self::Bytes {
                self.as_ustr().encode()
            }
        }

        impl EncodeKey for GreedyKey<UString<$uchar>> {
            type Bytes = Vec<u8>;

            fn encode(&self) -> Self::Bytes {
                let mut buf = Vec::with_capacity(::std::mem::size_of::<$uchar>() * self.len());

                for ch in self.as_slice() {
                    buf.extend_from_slice(&ch.to_be_bytes());
                }

                buf
            }
        }
    )+};
}

impl_widestr!(u16, u32);

// TODO: implement DecodeKey
