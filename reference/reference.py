#!/usr/bin/env python3
"""Slow, literal implementation of rules.md. The correctness oracle, not the submission.

Usage: reference.py [DATA_DIR OUTPUT_DIR]  (or DATA_DIR / OUTPUT_DIR env vars)
"""
import glob
import json
import os
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
DIGITS = frozenset("0123456789")


def parse_line(line, nfields):
    """Return the field list (None for null), or None if the line is malformed."""
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != nfields:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) < 2 or p[0] != '"' or p[-1] != '"' or '"' in p[1:-1]:
                return None
            p = p[1:-1]
        out.append(p if p != "" else None)
    return out


def read_rows(path, nfields):
    with open(path, "rb") as f:
        text = f.read().decode("utf-8", "surrogateescape")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # the final \n terminates the last line rather than starting an empty one
    for line in lines[1:]:  # first line is the header
        row = parse_line(line, nfields)
        if row is not None:
            yield row


def read_dir(data, sub, nfields):
    for path in sorted(glob.glob(os.path.join(data, sub, "*.csv"))):
        yield from read_rows(path, nfields)


def is_digits(s):
    return s != "" and all(c in DIGITS for c in s)


def days_from_civil(y, m, d):
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def civil_from_days(z):
    z += 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + (3 if mp < 10 else -9)
    return y + (m <= 2), m, d


def leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


MDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def parse_ts(s):
    """Epoch ms, or None if not one of the three valid forms."""
    if s is None:
        return None
    if is_digits(s):
        return int(s)
    # YYYY-MM-DDTHH:MM:SS then Z or +HH:MM / -HH:MM
    if len(s) == 20:
        body, zone = s[:19], s[19:]
        if zone != "Z":
            return None
        off = 0
    elif len(s) == 25:
        body, zone = s[:19], s[19:]
        if zone[0] not in "+-" or zone[3] != ":" or not is_digits(zone[1:3]) or not is_digits(zone[4:6]):
            return None
        oh, om = int(zone[1:3]), int(zone[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000 * (1 if zone[0] == "+" else -1)
    else:
        return None
    if body[4] != "-" or body[7] != "-" or body[10] != "T" or body[13] != ":" or body[16] != ":":
        return None
    nums = [body[0:4], body[5:7], body[8:10], body[11:13], body[14:16], body[17:19]]
    if not all(is_digits(n) for n in nums):
        return None
    y, mo, d, h, mi, se = map(int, nums)
    if not 1 <= mo <= 12 or h > 23 or mi > 59 or se > 59:
        return None
    dim = 29 if mo == 2 and leap(y) else MDAYS[mo - 1]
    if not 1 <= d <= dim:
        return None
    ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000
    return ms - off


class DSU:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        p = self.parent
        if x not in p:
            return x
        root = x
        while p[root] != root:
            root = p[root]
        while p[x] != root:
            p[x], x = root, p[x]
        return root

    def add(self, x):
        self.parent.setdefault(x, x)

    def union(self, a, b):
        self.add(a)
        self.add(b)
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # keep the byte-wise smallest ID as the root so the root is the person's name
            if ra < rb:
                self.parent[rb] = ra
            else:
                self.parent[ra] = rb


def main():
    data = sys.argv[1] if len(sys.argv) > 2 else os.environ.get("DATA_DIR", "/data")
    out = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("OUTPUT_DIR", "/output")
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    ws, we = parse_ts(manifest["window_start"]), parse_ts(manifest["window_end"])

    products = {r[0]: r[1] for r in read_rows(os.path.join(data, "products.csv"), 2) if r[0] is not None}
    campaigns = {r[0]: r[1] for r in read_rows(os.path.join(data, "campaigns.csv"), 2) if r[0] is not None}

    dsu = DSU()
    for a, b in read_dir(data, "links", 2):
        if a is not None and b is not None:
            dsu.union(a, b)

    # events: dedup on event_id by smallest ingest_ts (before validation)
    kept = {}
    for row in read_dir(data, "events", 7):
        eid = row[0]
        if eid is None:
            continue
        ing = parse_ts(row[3])
        prev = kept.get(eid)
        if prev is None or ing < prev[0]:
            kept[eid] = (ing, row)

    per_person = defaultdict(list)  # person -> [(ts, is_purchase, price, category)]
    for ing, (eid, uid, ts_s, _ing_s, etype, pid, price_s) in kept.values():
        if uid is None or pid is None or etype not in ("view", "cart", "purchase"):
            continue
        ts = parse_ts(ts_s)
        if ts is None or not ws <= ts < we:
            continue
        price = 0
        if etype == "purchase":
            if price_s is None or not is_digits(price_s):
                continue
            price = int(price_s)
            if not 1 <= price <= 10_000_000:
                continue
        if ing - ts > DAY_MS:
            continue
        per_person[dsu.find(uid)].append((ts, etype == "purchase", price, products.get(pid, "unknown")))

    clicks_by_id = {}
    for cid, uid, camp, ts_s in read_dir(data, "clicks", 4):
        if cid is None or cid in clicks_by_id:
            continue
        clicks_by_id[cid] = (uid, camp, ts_s)
    person_clicks = defaultdict(list)  # person -> [(ts, click_id, campaign)]
    for cid, (uid, camp, ts_s) in clicks_by_id.items():
        if uid is None or camp not in campaigns:
            continue
        ts = parse_ts(ts_s)
        if ts is None or not ws - WEEK_MS <= ts < we:
            continue
        person_clicks[dsu.find(uid)].append((ts, cid, camp))

    user_rows = {}  # (person, day) -> [sessions, events, revenue, {cat: cents}]
    camp_rows = {}  # (campaign, day) -> [sessions, purchasing, revenue, assisted]
    for person, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if not sessions or e[0] - sessions[-1][-1][0] > GAP_MS:
                sessions.append([e])
            else:
                sessions[-1].append(e)
        clicks = person_clicks.get(person, [])
        for sess in sessions:
            start = sess[0][0]
            day = start // DAY_MS
            rev = sum(e[2] for e in sess if e[1])
            purchased = any(e[1] for e in sess)
            u = user_rows.setdefault((person, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(sess)
            u[2] += rev
            for e in sess:
                if e[1]:
                    u[3][e[3]] += e[2]
            best = None
            assists = set()
            for ts, cid, camp in clicks:
                if start - WEEK_MS <= ts <= start:
                    assists.add(camp)
                    if best is None or ts > best[0] or (ts == best[0] and cid.encode("utf-8", "surrogateescape") < best[1].encode("utf-8", "surrogateescape")):
                        best = (ts, cid, camp)
            credited = best[2] if best else "organic"
            c = camp_rows.setdefault((credited, day), [0, 0, 0, 0])
            c[0] += 1
            c[1] += purchased
            c[2] += rev
            for camp in assists:
                camp_rows.setdefault((camp, day), [0, 0, 0, 0])[3] += 1

    def name_key(s):
        return s.encode("utf-8", "surrogateescape")

    def top(cats):
        if not cats:
            return None
        best = max(cats.values())
        return min((k for k, v in cats.items() if v == best), key=name_key)

    import datetime
    epoch = datetime.date(1970, 1, 1)
    keys = list(user_rows)
    ud = pa.table({
        "user_id": pa.array([k[0] for k in keys], pa.string()),
        "day": pa.array([epoch + datetime.timedelta(days=k[1]) for k in keys], pa.date32()),
        "sessions": pa.array([user_rows[k][0] for k in keys], pa.int64()),
        "events": pa.array([user_rows[k][1] for k in keys], pa.int64()),
        "revenue_cents": pa.array([user_rows[k][2] for k in keys], pa.int64()),
        "top_category": pa.array([top(user_rows[k][3]) for k in keys], pa.string()),
    })
    keys = list(camp_rows)
    cd = pa.table({
        "campaign_id": pa.array([k[0] for k in keys], pa.string()),
        "channel": pa.array([campaigns.get(k[0], "organic") if k[0] != "organic" else "organic" for k in keys], pa.string()),
        "day": pa.array([epoch + datetime.timedelta(days=k[1]) for k in keys], pa.date32()),
        "sessions": pa.array([camp_rows[k][0] for k in keys], pa.int64()),
        "purchasing_sessions": pa.array([camp_rows[k][1] for k in keys], pa.int64()),
        "revenue_cents": pa.array([camp_rows[k][2] for k in keys], pa.int64()),
        "assisted_sessions": pa.array([camp_rows[k][3] for k in keys], pa.int64()),
    })
    for name, table in (("user_daily", ud), ("campaign_daily", cd)):
        os.makedirs(os.path.join(out, name), exist_ok=True)
        pq.write_table(table, os.path.join(out, name, "part-0.parquet"))


if __name__ == "__main__":
    main()
