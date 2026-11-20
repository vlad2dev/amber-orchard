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

fn fields<const N: usize>(line: &[u8]) -> Option<[Bytes<'_>; N]> {
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
    let mut n = 0_i64;
    for &c in s {
        let digit = c.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(n)
}

#[inline]
fn two(s: &[u8], index: usize) -> Option<i64> {
    let a = s[index].wrapping_sub(b'0');
    let b = s[index + 1].wrapping_sub(b'0');
    (a <= 9 && b <= 9).then_some(i64::from(a) * 10 + i64::from(b))
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
            (hour * 60 + minute) * 60_000 * if sign == b'+' { 1 } else { -1 }
        }
        _ => return None,
    };
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = two(s, 0)? * 100 + two(s, 2)?;
    let month = two(s, 5)?;
    let date = two(s, 8)?;
    let hour = two(s, 11)?;
    let minute = two(s, 14)?;
    let second = two(s, 17)?;
    const MONTH_DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let max_date = *MONTH_DAYS.get((month - 1) as usize)?
        + i64::from(month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if date == 0 || date > max_date || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let march_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * march_month + 2) / 5 + date - 1;
    let days = era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100
        + day_of_year
        - 719_468;
    Some(days * DAY + ((hour * 60 + minute) * 60 + second) * 1000 - offset)
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
