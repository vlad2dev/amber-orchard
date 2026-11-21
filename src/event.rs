use crate::ids::Id;

pub trait EventData: Copy + Default + Send {
    fn encode(ts: i64, cents: u32, category: u32, origin: i64) -> Self;
    fn time_key(self) -> i64;
    fn timestamp(self, origin: i64) -> i64;
    fn cents(self) -> u32;
    fn category(self) -> usize;
}

#[derive(Clone, Copy, Default)]
pub struct Packed(u64);

impl Packed {
    const PRICE_BITS: u32 = 24;
    const CATEGORY_BITS: u32 = 9;
    const TIME_SHIFT: u32 = Self::PRICE_BITS + Self::CATEGORY_BITS;

    pub fn supports(start: i64, end: i64, categories: usize) -> bool {
        end.abs_diff(start) <= (1 << (64 - Self::TIME_SHIFT))
            && categories <= (1 << Self::CATEGORY_BITS)
    }
}

impl EventData for Packed {
    #[inline]
    fn encode(ts: i64, cents: u32, category: u32, origin: i64) -> Self {
        let relative = (ts - origin) as u64;
        Self(
            (relative << Self::TIME_SHIFT)
                | (u64::from(category) << Self::PRICE_BITS)
                | u64::from(cents),
        )
    }

    #[inline]
    fn time_key(self) -> i64 {
        (self.0 >> Self::TIME_SHIFT) as i64
    }

    #[inline]
    fn timestamp(self, origin: i64) -> i64 {
        origin + self.time_key()
    }

    #[inline]
    fn cents(self) -> u32 {
        (self.0 as u32) & ((1 << Self::PRICE_BITS) - 1)
    }

    #[inline]
    fn category(self) -> usize {
        ((self.0 >> Self::PRICE_BITS) as usize) & ((1 << Self::CATEGORY_BITS) - 1)
    }
}

#[derive(Clone, Copy, Default)]
pub struct Wide {
    ts: i64,
    cents: u32,
    category: u32,
}

impl EventData for Wide {
    fn encode(ts: i64, cents: u32, category: u32, _: i64) -> Self {
        Self {
            ts,
            cents,
            category,
        }
    }

    fn time_key(self) -> i64 {
        self.ts
    }

    fn timestamp(self, _: i64) -> i64 {
        self.ts
    }

    fn cents(self) -> u32 {
        self.cents
    }

    fn category(self) -> usize {
        self.category as usize
    }
}

#[derive(Clone, Copy, Default)]
pub struct Event<T> {
    pub person: Id,
    pub data: T,
}

pub struct CopyEvent<T> {
    pub ingest: i64,
    pub event: Event<T>,
}

#[cfg(test)]
mod tests {
    use super::{EventData, Packed};

    #[test]
    fn packed_layout_boundaries() {
        let origin = -123_456_789;
        let end = origin + (1 << 31);
        assert!(Packed::supports(origin, end, 512));
        assert!(!Packed::supports(origin, end + 1, 512));
        assert!(!Packed::supports(origin, end, 513));
        let event = Packed::encode(end - 1, 10_000_000, 511, origin);
        assert_eq!(event.timestamp(origin), end - 1);
        assert_eq!(event.cents(), 10_000_000);
        assert_eq!(event.category(), 511);
    }
}
