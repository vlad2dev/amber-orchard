mod ids;
mod input;
mod output;

use ids::{Id, Ids};
use input::{
    Bytes, DAY, LinkedIds, Map, Partitions, WEEK, Window, bucket, chunks, dictionary, map,
    mappings, number, partition, records, timestamp, transpose,
};
use rayon::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Clone, Copy)]
struct Event {
    person: Id,
    ts: i64,
    cents: u32,
    category: u32,
}

struct CopyEvent {
    ingest: i64,
    event: Event,
}

#[derive(Clone, Copy)]
struct Click<'a> {
    person: Id,
    id: Bytes<'a>,
    ts: i64,
    campaign: usize,
}

struct Catalog<'a> {
    products: Map<Bytes<'a>, u32>,
    categories: Vec<Option<Bytes<'a>>>,
    campaigns: Map<Bytes<'a>, usize>,
    campaign_names: Vec<(Bytes<'a>, Option<Bytes<'a>>)>,
}

impl<'a> Catalog<'a> {
    fn new(products: &'a [u8], campaigns: &'a [u8]) -> Self {
        let mut category_ids = map();
        let mut categories: Vec<Option<Bytes<'a>>> = vec![Some(b"unknown")];
        category_ids.insert(categories[0], 0);
        let products = dictionary(products)
            .map(|[id, name]| {
                let name = (!name.is_empty()).then_some(name);
                let index = *category_ids.entry(name).or_insert_with(|| {
                    let index = categories.len() as u32;
                    categories.push(name);
                    index
                });
                (id, index)
            })
            .collect();
        let mut campaign_names: Vec<(Bytes<'a>, Option<Bytes<'a>>)> =
            vec![(b"organic", Some(b"organic"))];
        let campaigns = dictionary(campaigns)
            .map(|[id, channel]| {
                let index = campaign_names.len();
                campaign_names.push((id, (!channel.is_empty()).then_some(channel)));
                (id, index)
            })
            .collect();
        Self {
            products,
            categories,
            campaigns,
            campaign_names,
        }
    }
}

fn parse_event<'a>(
    [_, user, ts, _, kind, product, price]: [Bytes<'a>; 7],
    ingest: i64,
    window: Window,
    ids: &Ids<'a>,
    catalog: &Catalog,
) -> Option<Event> {
    if user.is_empty() || product.is_empty() {
        return None;
    }
    let cents = match kind {
        b"purchase" => {
            let cents = number(price)?;
            if !(1..=10_000_000).contains(&cents) {
                return None;
            }
            cents as u32
        }
        b"view" | b"cart" => 0,
        _ => return None,
    };
    let ts = timestamp(ts)?;
    if !(window.start..window.end).contains(&ts) || ingest - ts > DAY {
        return None;
    }
    let user = ids.key(user);
    Some(Event {
        person: user,
        ts,
        cents,
        category: if cents == 0 {
            0
        } else {
            catalog.products.get(product).copied().unwrap_or(0)
        },
    })
}

fn partition_events<'a>(
    files: &'a [memmap2::Mmap],
    window: Window,
    ids: &Ids<'a>,
    catalog: &Catalog,
) -> Vec<Partitions<(Id, CopyEvent)>> {
    chunks(files)
        .into_par_iter()
        .map(|chunk| {
            let mut buckets = partition();
            for row in records::<7>(chunk) {
                let id = row[0];
                if id.is_empty() {
                    continue;
                }
                let id = ids.key(id);
                let ingest = timestamp(row[3]).expect("valid ingest timestamp");
                let event = parse_event(row, ingest, window, ids, catalog).unwrap_or(Event {
                    person: Id::default(),
                    ts: 0,
                    cents: 0,
                    category: 0,
                });
                buckets[bucket(id)].push((id, CopyEvent { ingest, event }));
            }
            buckets
        })
        .collect()
}

fn dedup_events(
    groups: Vec<Partitions<(Id, CopyEvent)>>,
    people: &LinkedIds,
) -> Vec<Vec<Vec<Event>>> {
    let start = Instant::now();
    let groups: Vec<Partitions<Event>> = transpose(groups)
        .into_par_iter()
        .map(|groups| {
            let mut earliest = map();
            earliest.reserve(groups.iter().map(Vec::len).sum());
            for (id, copy) in groups.into_iter().flatten() {
                let kept = earliest.entry(id).or_insert_with(|| CopyEvent {
                    ingest: copy.ingest,
                    event: copy.event,
                });
                if copy.ingest < kept.ingest {
                    *kept = copy;
                }
            }
            let mut partitions = partition();
            for copy in earliest.into_values() {
                if !copy.event.person.is_empty() {
                    let mut event = copy.event;
                    event.person = people.name(event.person);
                    partitions[bucket(event.person)].push(event);
                }
            }
            partitions
        })
        .collect();
    eprintln!(
        "event dedup and repartition: {:.3}s",
        start.elapsed().as_secs_f64()
    );
    transpose(groups)
}

fn read_clicks<'a>(
    files: &'a [memmap2::Mmap],
    window: Window,
    people: &LinkedIds,
    ids: &Ids<'a>,
    catalog: &Catalog,
) -> Vec<Vec<Vec<Click<'a>>>> {
    let groups: Vec<_> = chunks(files)
        .into_par_iter()
        .map(|chunk| {
            let mut partitions = partition();
            for [id, user, campaign, ts] in records::<4>(chunk) {
                if id.is_empty() || user.is_empty() {
                    continue;
                }
                let Some(&campaign) = catalog.campaigns.get(campaign) else {
                    continue;
                };
                let Some(ts) = timestamp(ts) else { continue };
                if !(window.start - WEEK..window.end).contains(&ts) {
                    continue;
                }
                let user = ids.key(user);
                let person = people.name(user);
                partitions[bucket(person)].push(Click {
                    person,
                    id,
                    ts,
                    campaign,
                });
            }
            partitions
        })
        .collect();
    transpose(groups)
}

#[derive(Default, Clone, Copy)]
struct CampaignTotals {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

type CampaignDays = Map<i32, Vec<CampaignTotals>>;

#[derive(Default)]
struct UserTotals<'a> {
    person: Id,
    day: i32,
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    top_category: Option<Bytes<'a>>,
}

struct ClickWindow {
    expired: usize,
    next: usize,
    active: Vec<usize>,
    counts: Vec<usize>,
    indices: Vec<usize>,
}

impl ClickWindow {
    fn new(campaigns: usize) -> Self {
        Self {
            expired: 0,
            next: 0,
            active: Vec::new(),
            counts: vec![0; campaigns],
            indices: vec![0; campaigns],
        }
    }

    fn clear(&mut self) {
        self.expired = 0;
        self.next = 0;
        for campaign in self.active.drain(..) {
            self.counts[campaign] = 0;
        }
    }

    fn advance(&mut self, clicks: &[Click], start: i64) -> usize {
        while self.expired < self.next && clicks[self.expired].ts < start - WEEK {
            let campaign = clicks[self.expired].campaign;
            self.counts[campaign] -= 1;
            if self.counts[campaign] == 0 {
                let index = self.indices[campaign];
                self.active.swap_remove(index);
                if let Some(&moved) = self.active.get(index) {
                    self.indices[moved] = index;
                }
            }
            self.expired += 1;
        }
        while let Some(click) = clicks.get(self.next).filter(|click| click.ts <= start) {
            if click.ts >= start - WEEK {
                let campaign = click.campaign;
                if self.counts[campaign] == 0 {
                    self.indices[campaign] = self.active.len();
                    self.active.push(campaign);
                }
                self.counts[campaign] += 1;
            } else {
                self.expired += 1;
            }
            self.next += 1;
        }
        if self.expired < self.next {
            clicks[self.next - 1].campaign
        } else {
            0
        }
    }
}

fn flatten<T>(groups: Vec<Vec<T>>) -> Vec<T> {
    let mut all = Vec::with_capacity(groups.iter().map(Vec::len).sum());
    for mut group in groups {
        all.append(&mut group);
    }
    all
}

fn summarize<'a>(
    event_groups: Vec<Vec<Event>>,
    click_groups: Vec<Vec<Click<'a>>>,
    catalog: &Catalog<'a>,
) -> (Vec<UserTotals<'a>>, CampaignDays) {
    let mut events = flatten(event_groups);
    let mut clicks = flatten(click_groups);
    events.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
    // The last click in a timestamp tie has the smallest click ID.
    clicks.sort_unstable_by(|a, b| {
        a.person
            .cmp(&b.person)
            .then(a.ts.cmp(&b.ts))
            .then_with(|| b.id.cmp(a.id))
    });
    clicks.dedup_by(|a, b| a.person == b.person && a.ts == b.ts && a.id == b.id);

    let mut users = Vec::new();
    let mut campaigns = map();
    let campaign_count = catalog.campaign_names.len();
    let mut click_window = ClickWindow::new(campaign_count);
    let mut click_offset = 0;
    let mut category_cents = vec![0_i64; catalog.categories.len()];
    let mut used_categories = Vec::new();
    for person_events in events.chunk_by(|a, b| a.person == b.person) {
        let person = person_events[0].person;
        while click_offset < clicks.len() && clicks[click_offset].person < person {
            click_offset += 1;
        }
        let click_start = click_offset;
        while click_offset < clicks.len() && clicks[click_offset].person == person {
            click_offset += 1;
        }
        let person_clicks = &clicks[click_start..click_offset];
        click_window.clear();

        let mut user = UserTotals {
            person,
            ..UserTotals::default()
        };
        let mut best_category: Option<usize> = None;
        for session in person_events.chunk_by(|a, b| b.ts - a.ts <= 1_800_000) {
            let start = session[0].ts;
            let day = start.div_euclid(DAY) as i32;
            if user.sessions > 0 && user.day != day {
                user.top_category = best_category.and_then(|index| catalog.categories[index]);
                users.push(user);
                user = UserTotals {
                    person,
                    ..UserTotals::default()
                };
                for category in used_categories.drain(..) {
                    category_cents[category] = 0;
                }
                best_category = None;
            }
            user.day = day;
            user.sessions += 1;
            user.events += session.len() as i64;
            let mut revenue = 0;
            for event in session.iter().filter(|event| event.cents != 0) {
                let category = event.category as usize;
                if category_cents[category] == 0 {
                    used_categories.push(category);
                }
                let price = i64::from(event.cents);
                category_cents[category] += price;
                revenue += price;
                if best_category.is_none_or(|best| {
                    category_cents[category] > category_cents[best]
                        || (category_cents[category] == category_cents[best]
                            && catalog.categories[category] < catalog.categories[best])
                }) {
                    best_category = Some(category);
                }
            }
            user.revenue_cents += revenue;

            let winner = click_window.advance(person_clicks, start);
            let day_campaigns: &mut Vec<CampaignTotals> = campaigns
                .entry(day)
                .or_insert_with(|| vec![CampaignTotals::default(); campaign_count]);
            let credited = &mut day_campaigns[winner];
            credited.sessions += 1;
            credited.purchasing_sessions += i64::from(revenue != 0);
            credited.revenue_cents += revenue;
            for &campaign in &click_window.active {
                day_campaigns[campaign].assisted_sessions += 1;
            }
        }
        user.top_category = best_category.and_then(|index| catalog.categories[index]);
        users.push(user);
        for category in used_categories.drain(..) {
            category_cents[category] = 0;
        }
    }
    (users, campaigns)
}

fn main() {
    let begin = Instant::now();
    let data = env_path("DATA_DIR", "/data");
    let out = env_path("OUTPUT_DIR", "/output");
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|n| n.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .expect("start workers");

    let window = Window::read(&data);
    let products = fs::read(data.join("products.csv")).expect("read products");
    let campaigns = fs::read(data.join("campaigns.csv")).expect("read campaigns");
    let catalog = Catalog::new(&products, &campaigns);
    let links = mappings(&data.join("links"));
    let ids = Ids::new();
    let event_files = mappings(&data.join("events"));
    let click_files = mappings(&data.join("clicks"));
    eprintln!(
        "catalog and input maps: {:.3}s",
        begin.elapsed().as_secs_f64()
    );

    let stage = Instant::now();
    // Shopping Sessions pipeline provenance: f67fef44-94d9-4b90-8635-3825b90fea16
    let (people, copies) = rayon::join(
        || input::people(&links, &ids),
        || partition_events(&event_files, window, &ids, &catalog),
    );
    eprintln!(
        "links and event parse: {:.3}s",
        stage.elapsed().as_secs_f64()
    );
    let events = dedup_events(copies, &people);
    let stage = Instant::now();
    let clicks = read_clicks(&click_files, window, &people, &ids, &catalog);
    eprintln!(
        "click parse and partition: {:.3}s",
        stage.elapsed().as_secs_f64()
    );

    let stage = Instant::now();
    let user_folder = out.join("user_daily");
    fs::create_dir_all(&user_folder).expect("create user output");
    let results: Vec<_> = events
        .into_par_iter()
        .zip(clicks)
        .enumerate()
        .map(|(index, (events, clicks))| {
            let (rows, campaigns) = summarize(events, clicks, &catalog);
            if !rows.is_empty() {
                output::write_users(&user_folder, index, &rows, &ids);
            }
            (rows.len(), campaigns)
        })
        .collect();
    if results.iter().all(|(count, _)| *count == 0) {
        output::write_users(&user_folder, 0, &[], &ids);
    }
    let mut campaigns = CampaignDays::default();
    for (_, partition) in results {
        for (day, totals) in partition {
            let target = campaigns
                .entry(day)
                .or_insert_with(|| vec![CampaignTotals::default(); totals.len()]);
            for (dst, src) in target.iter_mut().zip(totals) {
                dst.sessions += src.sessions;
                dst.purchasing_sessions += src.purchasing_sessions;
                dst.revenue_cents += src.revenue_cents;
                dst.assisted_sessions += src.assisted_sessions;
            }
        }
    }
    output::write_campaigns(&out, &catalog, campaigns);
    eprintln!(
        "sort, aggregate and write: {:.3}s",
        stage.elapsed().as_secs_f64()
    );
    eprintln!(
        "total process: {:.3}s ({threads} threads)",
        begin.elapsed().as_secs_f64()
    );
}

fn env_path(key: &str, default: &str) -> PathBuf {
    std::env::var_os(key).map_or_else(|| Path::new(default).to_owned(), PathBuf::from)
}
