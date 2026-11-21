use crate::ids::{Id, Ids};
use ahash::RandomState;
use memchr::memchr;
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;
use std::sync::LazyLock;

pub type Bytes<'a> = &'a [u8];
pub type Map<K, V> = HashMap<K, V, RandomState>;
pub const BUCKETS: usize = 64;
pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
const CHUNK: usize = 16 * 1024 * 1024;
static HASH: LazyLock<RandomState> = LazyLock::new(|| RandomState::with_seeds(1, 2, 3, 4));

pub fn map<K, V>() -> Map<K, V> {
    HashMap::with_hasher(HASH.clone())
}

pub fn bucket(key: impl std::hash::Hash) -> usize {
    HASH.hash_one(key) as usize & (BUCKETS - 1)
}

pub fn mappings(path: &Path) -> Vec<Mmap> {
    let mut files: Vec<_> = fs::read_dir(path)
        .expect("read input directory")
        .map(|entry| entry.expect("read input directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "csv"))
        .collect();
    files.sort_unstable();
    files
        .iter()
        .filter_map(|path| {
            let file = File::open(path).expect("open CSV");
            if file.metadata().expect("read CSV metadata").len() == 0 {
                return None;
            }
            // The scoring input is read-only for the life of this process.
            Some(unsafe { MmapOptions::new().map(&file) }.expect("map CSV"))
        })
        .collect()
}

pub fn chunks(files: &[Mmap]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for file in files {
        let Some(header) = memchr(b'\n', file) else {
            continue;
        };
        let mut rest = &file[header + 1..];
        while rest.len() > CHUNK {
            let boundary =
                memchr(b'\n', &rest[CHUNK..]).map_or(rest.len(), |offset| CHUNK + offset + 1);
            let (chunk, tail) = rest.split_at(boundary);
            chunks.push(chunk);
            rest = tail;
        }
        if !rest.is_empty() {
            chunks.push(rest);
        }
    }
    // Start large jobs earlier to reduce the tail when shards are uneven.
    chunks.sort_unstable_by_key(|chunk| std::cmp::Reverse(chunk.len()));
    chunks
}

pub fn records<const N: usize>(mut chunk: &[u8]) -> impl Iterator<Item = [Bytes<'_>; N]> {
    std::iter::from_fn(move || {
        while !chunk.is_empty() {
            let (consumed, row) = scan::<N>(chunk);
            chunk = &chunk[consumed..];
            if row.is_some() {
                return row;
            }
        }
        None
    })
}

#[inline]
fn scan<const N: usize>(input: &[u8]) -> (usize, Option<[Bytes<'_>; N]>) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if N >= 4 && input.len() >= 128 {
        use std::arch::x86_64::*;
        let mut commas = 0_u128;
        let mut quotes = 0_u128;
        for offset in [0, 32, 64, 96] {
            // The length check above guarantees a readable 32-byte block.
            let (newlines, separators, delimiters) = unsafe {
                let bytes = _mm256_loadu_si256(input.as_ptr().add(offset).cast());
                (
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'\n' as i8)))
                        as u32,
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8)))
                        as u32,
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8)))
                        as u32,
                )
            };
            if newlines != 0 {
                let end = newlines.trailing_zeros();
                let keep = (1_u32 << end) - 1;
                commas |= u128::from(separators & keep) << offset;
                quotes |= u128::from(delimiters & keep) << offset;
                let end = offset + end as usize;
                return (end + 1, masked_fields(&input[..end], commas, quotes));
            }
            commas |= u128::from(separators) << offset;
            quotes |= u128::from(delimiters) << offset;
        }
        let end = memchr(b'\n', &input[128..]).map_or(input.len(), |offset| offset + 128);
        return ((end + 1).min(input.len()), scalar_fields(&input[..end]));
    }

    let end = memchr(b'\n', input).unwrap_or(input.len());
    ((end + 1).min(input.len()), scalar_fields(&input[..end]))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn masked_fields<const N: usize>(
    line: &[u8],
    mut commas: u128,
    quotes: u128,
) -> Option<[Bytes<'_>; N]> {
    if line.is_empty() {
        return None;
    }
    let boundaries = (commas << 1) | (commas >> 1) | 1 | (1_u128 << (line.len() - 1));
    if commas.count_ones() as usize != N - 1 || quotes & !boundaries != 0 {
        return None;
    }
    let mut result: [Bytes<'_>; N] = [&[]; N];
    let mut start = 0;
    for (index, field) in result.iter_mut().enumerate() {
        let end = if index + 1 == N {
            line.len()
        } else {
            let comma = commas.trailing_zeros() as usize;
            commas &= commas - 1;
            comma
        };
        let raw = &line[start..end];
        let leading = raw.first() == Some(&b'"');
        let trailing = raw.last() == Some(&b'"');
        let trim = usize::from(leading);
        if leading != trailing || raw.len() < 2 * trim {
            return None;
        }
        *field = &raw[trim..raw.len() - trim];
        start = end + 1;
    }
    Some(result)
}

fn scalar_fields<const N: usize>(line: &[u8]) -> Option<[Bytes<'_>; N]> {
    let mut commas = memchr::memchr_iter(b',', line);
    let mut fields: [Bytes<'_>; N] = [&[]; N];
    let mut start = 0;
    for (index, field) in fields.iter_mut().enumerate() {
        let end = if index + 1 == N {
            line.len()
        } else {
            commas.next()?
        };
        let raw = &line[start..end];
        *field = if raw.first() == Some(&b'"') {
            if raw.len() < 2 || raw.last() != Some(&b'"') {
                return None;
            }
            &raw[1..raw.len() - 1]
        } else {
            raw
        };
        if memchr(b'"', field).is_some() {
            return None;
        }
        start = end + 1;
    }
    if commas.next().is_some() {
        return None;
    }
    Some(fields)
}

pub fn dictionary(data: &[u8]) -> impl Iterator<Item = [Bytes<'_>; 2]> {
    let body = memchr(b'\n', data).map_or(&[][..], |offset| &data[offset + 1..]);
    records(body).filter(|row| !row[0].is_empty())
}

pub fn number(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    // Eighteen decimal digits always fit in i64.
    let mut n = 0_i64;
    if s.len() <= 18 {
        for &c in s {
            let digit = c.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            n = n * 10 + i64::from(digit);
        }
    } else {
        for &c in s {
            let digit = c.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            n = n.checked_mul(10)?.checked_add(i64::from(digit))?;
        }
    }
    Some(n)
}

#[inline]
fn two(s: &[u8], index: usize) -> Option<i32> {
    let a = s[index].wrapping_sub(b'0');
    let b = s[index + 1].wrapping_sub(b'0');
    (a <= 9 && b <= 9).then_some(i32::from(a) * 10 + i32::from(b))
}

const fn month_start(year: i32, month: i32) -> i32 {
    let year = year - (month <= 2) as i32;
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let march_month = if month > 2 { month - 3 } else { month + 9 };
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100
        + (153 * march_month + 2) / 5
        - 719_468
}

const fn calendar() -> [[i32; 13]; 10_000] {
    let mut table = [[0; 13]; 10_000];
    let mut year = 0;
    while year < 10_000 {
        let mut month = 1;
        while month <= 12 {
            let start = month_start(year as i32, month as i32);
            let next = if month == 12 {
                month_start(year as i32 + 1, 1)
            } else {
                month_start(year as i32, month as i32 + 1)
            };
            table[year][month] = (start << 5) | (next - start);
            month += 1;
        }
        year += 1;
    }
    table
}

static CALENDAR: [[i32; 13]; 10_000] = calendar();

#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline]
fn civil_digits(s: &[u8]) -> Option<[i32; 6]> {
    use std::arch::x86_64::*;

    // The input is at least 20 bytes. The shuffle removes the separators and
    // the final two digits are parsed by two() below.
    let pairs = unsafe {
        let raw = _mm_loadu_si128(s.as_ptr().cast());
        let order = _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1);
        let digits = _mm_sub_epi8(_mm_shuffle_epi8(raw, order), _mm_set1_epi8(b'0' as i8));
        let valid = _mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9)));
        if _mm_movemask_epi8(valid) & 0xfff != 0xfff {
            return None;
        }
        _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a))
    };
    Some(unsafe {
        [
            _mm_extract_epi16::<0>(pairs) * 100 + _mm_extract_epi16::<1>(pairs),
            _mm_extract_epi16::<2>(pairs),
            _mm_extract_epi16::<3>(pairs),
            _mm_extract_epi16::<4>(pairs),
            _mm_extract_epi16::<5>(pairs),
            two(s, 17)?,
        ]
    })
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
fn civil_digits(s: &[u8]) -> Option<[i32; 6]> {
    Some([
        two(s, 0)? * 100 + two(s, 2)?,
        two(s, 5)?,
        two(s, 8)?,
        two(s, 11)?,
        two(s, 14)?,
        two(s, 17)?,
    ])
}

pub fn timestamp(s: &[u8]) -> Option<i64> {
    if !matches!(s.len(), 20 | 25) || s[4] != b'-' {
        return number(s);
    }

    let offset = match (s.len(), s[19]) {
        (20, b'Z') => 0,
        (25, sign @ (b'+' | b'-')) if s[22] == b':' => {
            let hour = two(s, 20)?;
            let minute = two(s, 23)?;
            if hour > 23 || minute > 59 {
                return None;
            }
            (hour * 60 + minute) * 60 * if sign == b'+' { 1 } else { -1 }
        }
        _ => return None,
    };
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }

    let [year, month, date, hour, minute, second] = civil_digits(s)?;
    let calendar = *CALENDAR[year as usize].get(month as usize)?;
    if date == 0 || date > (calendar & 31) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = i64::from((calendar >> 5) + date - 1);
    let seconds = (hour * 60 + minute) * 60 + second - offset;
    Some(days * DAY + i64::from(seconds) * 1000)
}

#[derive(Clone, Copy)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

impl Window {
    pub fn read(data: &Path) -> Self {
        let bytes = fs::read(data.join("manifest.json")).expect("read manifest");
        let manifest: serde_json::Value = serde_json::from_slice(&bytes).expect("parse manifest");
        let at = |field: &str| {
            timestamp(
                manifest[field]
                    .as_str()
                    .expect("manifest timestamp")
                    .as_bytes(),
            )
            .expect("valid manifest timestamp")
        };
        Self {
            start: at("window_start"),
            end: at("window_end"),
        }
    }
}

// Fixed-width UTC ingest strings sort in chronological order. For small
// windows, keep this order key and compare against precomputed next-day dates.
pub struct IngestOrder {
    first_day: i64,
    next_dates: Vec<i64>,
}

impl IngestOrder {
    pub fn new(window: Window) -> Self {
        let first_day = window.start.div_euclid(DAY);
        let last_day = window.end.div_euclid(DAY);
        let mut next_dates = Vec::new();
        if (0..=4096).contains(&(last_day - first_day))
            && first_day + 1 >= i64::from(month_start(0, 1))
            && last_day + 1 < i64::from(month_start(10_000, 1))
        {
            for next_day in first_day + 1..=last_day + 1 {
                let year =
                    CALENDAR.partition_point(|months| i64::from(months[1] >> 5) <= next_day) - 1;
                let months = &CALENDAR[year][1..];
                let month = months.partition_point(|start| i64::from(start >> 5) <= next_day);
                let day = next_day - i64::from(months[month - 1] >> 5) + 1;
                next_dates.push(
                    (bcd(year as i64 / 100) << 24)
                        | (bcd(year as i64 % 100) << 16)
                        | (bcd(month as i64) << 8)
                        | bcd(day),
                );
            }
        }
        Self {
            first_day,
            next_dates,
        }
    }

    #[inline]
    pub fn key(&self, s: &[u8]) -> i64 {
        if self.next_dates.is_empty() {
            return timestamp(s).expect("valid ingest timestamp");
        }
        // The specification guarantees an F1 ingest timestamp on every
        // well-formed event row. Pack only its digits, preserving their order.
        let prefix = u64::from_be_bytes(s[..8].try_into().expect("valid ingest timestamp"));
        let middle = u64::from_be_bytes(s[8..16].try_into().expect("valid ingest timestamp"));
        let date = compress_digits(prefix, 0x0f0f_0f0f_000f_0f00);
        let time = compress_digits(middle, 0x0f0f_000f_0f00_0f0f);
        ((date << 32) | (time << 8) | (u64::from(s[17] & 15) << 4) | u64::from(s[18] & 15)) as i64
    }

    #[inline]
    pub fn is_late(&self, key: i64, ts: i64) -> bool {
        if self.next_dates.is_empty() {
            return key - ts > DAY;
        }
        let day = ts.div_euclid(DAY);
        let next_date = self.next_dates[(day - self.first_day) as usize];
        let ingest_date = key >> 24;
        if ingest_date != next_date {
            return ingest_date > next_date;
        }
        // An ingest timestamp has whole seconds. Comparing to the floor
        // of the threshold gives the same strict comparison at millisecond precision.
        let seconds = ts.rem_euclid(DAY) / 1000;
        let threshold =
            (bcd(seconds / 3600) << 16) | (bcd(seconds / 60 % 60) << 8) | bcd(seconds % 60);
        key & 0xff_ffff > threshold
    }
}

#[inline]
fn bcd(value: i64) -> i64 {
    ((value / 10) << 4) | value % 10
}

#[inline]
fn compress_digits(bytes: u64, mask: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // BMI2 is guaranteed by the image's CPU target.
        unsafe { std::arch::x86_64::_pext_u64(bytes, mask) }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut result = 0;
        for shift in (0..8).rev().map(|byte| byte * 8) {
            if mask >> shift & 15 != 0 {
                result = (result << 4) | (bytes >> shift & 15);
            }
        }
        result
    }
}

fn root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

pub struct LinkedIds {
    indices: Map<Id, usize>,
    names: Vec<Id>,
}

impl LinkedIds {
    #[inline]
    pub fn name(&self, id: Id) -> Id {
        self.indices.get(&id).map_or(id, |&index| self.names[index])
    }
}

pub fn people<'a>(files: &'a [Mmap], ids: &Ids<'a>) -> LinkedIds {
    let edges: Vec<Vec<_>> = chunks(files)
        .into_par_iter()
        .map(|chunk| {
            records::<2>(chunk)
                .filter(|[a, b]| !a.is_empty() && !b.is_empty())
                .map(|[a, b]| [(ids.key(a), a), (ids.key(b), b)])
                .collect()
        })
        .collect();
    let edge_count = edges.iter().map(Vec::len).sum::<usize>();
    let capacity = edge_count + edge_count / 2;
    let mut indices = map();
    indices.reserve(capacity);
    let mut names = Vec::with_capacity(capacity);
    let mut parents = Vec::with_capacity(capacity);
    let mut sizes = Vec::with_capacity(capacity);
    for [a, b] in edges.into_iter().flatten() {
        let mut intern = |name: (Id, Bytes<'a>)| {
            *indices.entry(name.0).or_insert_with(|| {
                let index = parents.len();
                parents.push(index);
                names.push(name);
                sizes.push(1);
                index
            })
        };
        let a = intern(a);
        let b = intern(b);
        let mut a = root(&mut parents, a);
        let mut b = root(&mut parents, b);
        if a != b {
            if sizes[a] < sizes[b] {
                std::mem::swap(&mut a, &mut b);
            }
            parents[b] = a;
            sizes[a] += sizes[b];
            if names[b].1 < names[a].1 {
                names[a] = names[b];
            }
        }
    }
    let names = (0..parents.len())
        .map(|index| names[root(&mut parents, index)].0)
        .collect();
    LinkedIds { indices, names }
}

pub type Partitions<T> = [Vec<T>; BUCKETS];

pub fn partition<T>() -> Partitions<T> {
    std::array::from_fn(|_| Vec::new())
}

pub fn transpose<T>(groups: Vec<Partitions<T>>) -> Vec<Vec<Vec<T>>> {
    let mut buckets: Vec<Vec<Vec<T>>> = (0..BUCKETS).map(|_| Vec::new()).collect();
    for group in groups {
        for (bucket, rows) in buckets.iter_mut().zip(group) {
            if !rows.is_empty() {
                bucket.push(rows);
            }
        }
    }
    buckets
}

#[cfg(test)]
mod tests {
    use super::{scalar_fields, scan};

    fn compare(line: &[u8]) {
        let mut input = line.to_vec();
        input.push(b'\n');
        input.extend([b'"'; 128]);
        let (consumed, parsed) = scan::<4>(&input);
        assert_eq!(consumed, line.len() + 1);
        assert_eq!(parsed, scalar_fields::<4>(line));
    }

    #[test]
    fn fixed_width_ingest_keys_match_timestamp_order_and_lateness() {
        use super::{DAY, IngestOrder, Window, timestamp};

        let start = timestamp(b"1999-12-28T00:00:00Z").unwrap();
        let end = timestamp(b"2000-03-04T00:00:00Z").unwrap();
        let order = IngestOrder::new(Window { start, end });
        let strings: &[&[u8]] = &[
            b"1999-12-31T23:59:59Z",
            b"2000-01-01T00:00:00Z",
            b"2000-01-03T09:12:58Z",
            b"2000-02-29T00:00:00Z",
            b"2000-03-01T13:06:24Z",
        ];
        for pair in strings.windows(2) {
            assert!(order.key(pair[0]) < order.key(pair[1]));
        }
        for text in strings {
            let ingest = timestamp(text).unwrap();
            let key = order.key(text);
            for delta in [-DAY - 1, -DAY, -DAY + 1, -1, 0, 1] {
                assert_eq!(order.is_late(key, ingest + delta), -delta > DAY);
            }
        }

        let fallback = IngestOrder::new(Window { start: 0, end });
        assert_eq!(fallback.key(strings[0]), timestamp(strings[0]).unwrap());
    }

    #[test]
    fn delimiter_masks_match_scalar_scan() {
        for length in 0..=160 {
            for separator in 0..length {
                for quoted in [false, true] {
                    let mut line = b"a,b,".to_vec();
                    line.extend(vec![b'x'; length]);
                    line[separator + 4] = b',';
                    if quoted && separator >= 2 {
                        line[4] = b'"';
                        line[separator + 3] = b'"';
                    }
                    compare(&line);
                    for position in [0, 31, 32, 63, 64, 95, 96, 127, length + 3] {
                        if position >= line.len() || position == separator + 4 {
                            continue;
                        }
                        let saved = line[position];
                        for byte in [b'"', b',', b'\r'] {
                            line[position] = byte;
                            compare(&line);
                        }
                        line[position] = saved;
                    }
                }
            }
        }
    }
}
