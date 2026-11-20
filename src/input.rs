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

pub fn bucket(key: &[u8]) -> usize {
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

pub fn records<const N: usize>(chunk: &[u8]) -> impl Iterator<Item = [Bytes<'_>; N]> {
    let mut start = 0;
    memchr::memchr_iter(b'\n', chunk)
        .chain(std::iter::once(chunk.len()))
        .filter_map(move |end| {
            if start > end {
                return None;
            }
            let line = &chunk[start..end];
            start = end + 1;
            fields(line)
        })
}

// Expected input rows fit in two 128-bit delimiter masks. Longer rows use the
// same dialect via the portable scanner, with no limit on field or row length.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn fields<const N: usize>(line: &[u8]) -> Option<[Bytes<'_>; N]> {
    use std::arch::x86_64::*;

    if !(32..=128).contains(&line.len()) {
        return scalar_fields(line);
    }

    #[inline]
    fn masks(block: &[u8], offset: usize) -> (u32, u32) {
        // Every caller passes an offset with at least 32 readable bytes. The
        // Docker target enables AVX2, and loadu accepts unaligned addresses.
        unsafe {
            let bytes = _mm256_loadu_si256(block.as_ptr().add(offset).cast());
            let commas = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8));
            let quotes = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8));
            (
                _mm256_movemask_epi8(commas) as u32,
                _mm256_movemask_epi8(quotes) as u32,
            )
        }
    }

    let (first_commas, first_quotes) = masks(line, 0);
    let mut commas = u128::from(first_commas);
    let mut quotes = u128::from(first_quotes);
    let mut offset = 32;
    while offset + 32 <= line.len() {
        let (more_commas, more_quotes) = masks(line, offset);
        commas |= u128::from(more_commas) << offset;
        quotes |= u128::from(more_quotes) << offset;
        offset += 32;
    }
    if offset < line.len() {
        // The final vector overlaps the previous one. ORing its mask keeps
        // each delimiter at its original byte index.
        let offset = line.len() - 32;
        let (last_commas, last_quotes) = masks(line, offset);
        commas |= u128::from(last_commas) << offset;
        quotes |= u128::from(last_quotes) << offset;
    }
    if commas.count_ones() as usize != N - 1 {
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
        *field = if raw.len() >= 2 && raw[0] == b'"' && raw[raw.len() - 1] == b'"' {
            quotes ^= (1_u128 << start) | (1_u128 << (end - 1));
            &raw[1..raw.len() - 1]
        } else {
            raw
        };
        start = end + 1;
    }
    // Quotes anywhere other than the two boundaries of a field are malformed.
    (quotes == 0).then_some(result)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
use scalar_fields as fields;

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

fn root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

pub fn people(files: &[Mmap]) -> Map<Bytes<'_>, Bytes<'_>> {
    let edges: Vec<Vec<_>> = chunks(files)
        .into_par_iter()
        .map(|chunk| {
            records::<2>(chunk)
                .filter(|[a, b]| !a.is_empty() && !b.is_empty())
                .collect()
        })
        .collect();
    let mut ids = map();
    let mut names = Vec::new();
    let mut parents = Vec::new();
    let mut sizes = Vec::new();
    for [a, b] in edges.into_iter().flatten() {
        let mut intern = |id| {
            *ids.entry(id).or_insert_with(|| {
                let index = parents.len();
                parents.push(index);
                names.push(id);
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
            names[a] = names[a].min(names[b]);
        }
    }
    ids.into_iter()
        .map(|(id, index)| (id, names[root(&mut parents, index)]))
        .collect()
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
    use super::{fields, scalar_fields};

    #[test]
    fn delimiter_masks_match_scalar_scan() {
        // Sweep the separator, leading/trailing quotes and CR across vector
        // boundaries, including the overlap in a partial final block.
        for length in 0..=160 {
            for comma in 0..length {
                for quoted in [false, true] {
                    let mut line = vec![b'x'; length];
                    line[comma] = b',';
                    if quoted && comma >= 2 {
                        line[0] = b'"';
                        line[comma - 1] = b'"';
                    }
                    assert_eq!(fields::<2>(&line), scalar_fields::<2>(&line));
                    for stray in [0, 31, 32, 63, 64, 95, 96, 127, length - 1] {
                        if stray >= length || stray == comma {
                            continue;
                        }
                        let old = line[stray];
                        for byte in [b'"', b',', b'\r'] {
                            line[stray] = byte;
                            assert_eq!(fields::<2>(&line), scalar_fields::<2>(&line));
                        }
                        line[stray] = old;
                    }
                }
            }
        }
    }
}
