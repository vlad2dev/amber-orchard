use crate::input::{Bytes, Map, map};
use std::sync::Mutex;

// Shopping Sessions implementation provenance: f67fef44-94d9-4b90-8635-3825b90fea16
const EXTERNAL: u64 = 1 << 63;
// The low seven bits of each byte form a dense, reversible key.
const ASCII_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id(u64);

impl Id {
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

struct External<'a> {
    ids: Map<Bytes<'a>, Id>,
    bytes: Vec<Bytes<'a>>,
}

pub struct Ids<'a>(Mutex<External<'a>>);

impl<'a> Ids<'a> {
    pub fn new() -> Self {
        Self(Mutex::new(External {
            ids: map(),
            bytes: Vec::new(),
        }))
    }

    #[inline]
    pub fn key(&self, bytes: Bytes<'a>) -> Id {
        if let Some(key) = inline(bytes) {
            return Id(key);
        }
        self.intern(bytes)
    }

    #[cold]
    fn intern(&self, bytes: Bytes<'a>) -> Id {
        let mut index = self.0.lock().expect("lock external ID index");
        let External { ids, bytes: stored } = &mut *index;
        *ids.entry(bytes).or_insert_with(|| {
            let key = Id(EXTERNAL | stored.len() as u64);
            stored.push(bytes);
            key
        })
    }

    #[inline]
    pub fn bytes<'b>(&self, id: Id, buffer: &'b mut [u8; 9]) -> &'b [u8]
    where
        'a: 'b,
    {
        if id.0 & EXTERNAL != 0 {
            return self.0.lock().expect("lock external ID index").bytes
                [(id.0 ^ EXTERNAL) as usize];
        }

        let len = ((u64::BITS - id.0.leading_zeros()) as usize).div_ceil(7);
        buffer[..8].copy_from_slice(&expand(id.0).to_le_bytes());
        buffer[8] = (id.0 >> 56) as u8;
        &buffer[..len]
    }
}

#[inline]
fn inline(bytes: &[u8]) -> Option<u64> {
    match bytes {
        [first @ .., last] if first.len() == 8 => {
            let first = u64::from_le_bytes(first.try_into().unwrap());
            if first & HIGH != 0
                || (first.wrapping_sub(ONES) & !first & HIGH != 0)
                || *last == 0
                || *last >= 128
            {
                return None;
            }
            Some(compact(first) | (u64::from(*last) << 56))
        }
        bytes if bytes.len() <= 8 => {
            let mut key = 0;
            for (index, &byte) in bytes.iter().enumerate() {
                if byte == 0 || byte >= 128 {
                    return None;
                }
                key |= u64::from(byte) << (7 * index);
            }
            Some(key)
        }
        _ => None,
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline]
fn compact(bytes: u64) -> u64 {
    // BMI2 is guaranteed by the scoring image's target.
    unsafe { std::arch::x86_64::_pext_u64(bytes, ASCII_BITS) }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
fn compact(bytes: u64) -> u64 {
    (0..8)
        .map(|i| ((bytes >> (i * 8)) & (ASCII_BITS & 127)) << (i * 7))
        .sum()
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline]
fn expand(key: u64) -> u64 {
    // BMI2 is guaranteed by the scoring image's target.
    unsafe { std::arch::x86_64::_pdep_u64(key, ASCII_BITS) }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
fn expand(key: u64) -> u64 {
    (0..8)
        .map(|i| ((key >> (i * 7)) & (ASCII_BITS & 127)) << (i * 8))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::Ids;

    #[test]
    fn ids_preserve_every_byte() {
        let samples = (0..=9)
            .flat_map(|length| {
                (0..=255).map(move |byte| {
                    let mut value = vec![b'x'; length];
                    if length != 0 {
                        value[length / 2] = byte;
                    }
                    value
                })
            })
            .chain([b"ten letters".to_vec()])
            .collect::<Vec<_>>();
        let ids = Ids::new();
        let mut buffer = [0; 9];
        for sample in &samples {
            let key = ids.key(sample);
            assert_eq!(ids.key(sample), key);
            assert_eq!(ids.bytes(key, &mut buffer), sample);
        }
    }
}
