# Shopping Sessions

This image reads the scoring input from `DATA_DIR` and writes
`user_daily` and `campaign_daily` to `OUTPUT_DIR`. It uses `NCPU` Rayon
threads and writes uncompressed Parquet with `DATE` day columns.

## Implementation

The reader memory-maps every CSV shard and distributes newline-aligned chunks
across Rayon workers. The CSV parser preserves carriage returns and spaces,
strips paired field quotes, and drops malformed rows. AVX2 scans comma and quote
masks for 32–128-byte rows. The scalar scanner handles other row lengths.
Linked IDs are resolved up front with union-find, using the byte-wise minimum
ID as the person name.

Events are partitioned by event ID and reduced to the earliest ingest.
Malformed copies never enter the reduction. An invalid but well-formed earliest
copy still wins the reduction and drops the event. Surviving events and valid
clicks are partitioned by canonical person. Each partition sorts timestamps,
aggregates sessions on their UTC start date, and attributes sessions using a
rolling seven-day click window. The window maintains per-campaign counts so
each qualifying campaign assists once per session. Click copies are identical
by contract and collapse within their person's partition.

F3 values above the signed 64-bit range cannot be in the manifest window. The
implementation discards them during parsing, which yields the same rows as the
reference's arbitrary-precision parse followed by its window check.

## Verify

Use Bash for these commands. On the Windows development machine, use Git Bash
and set `PY=.venv/Scripts/python.exe`. Input data and generated output live
outside the repository.

```sh
PY=${PY:-python3}
DATA_ROOT=${DATA_ROOT:-../ss-data}
ANSWER=../buildathon-downloads/shopping-sessions/x/dev-answer/dev-answer

docker build --platform linux/amd64 -t shopping-sessions:latest .
"$PY" scripts/run_fixtures.py fixtures -- bash scripts/docker_run.sh

bash scripts/docker_run.sh "$DATA_ROOT/dev1" "$DATA_ROOT/rust-dev1"
"$PY" scripts/compare.py "$DATA_ROOT/rust-dev1" "$ANSWER"
```

Generate the two independent oracle inputs and the 20M-event timing input with
the supplied Linux generator. Set `DATA_ROOT_ABS` to the absolute, Docker-visible
path of `DATA_ROOT`. In Git Bash, use `cygpath -am "$DATA_ROOT"` for that path
and export `MSYS_NO_PATHCONV=1` for the three generator commands.

```sh
docker run --rm --cpus 4 -v "$DATA_ROOT_ABS:/out" rust:1.85 \
  /out/generate --seed 7 --events 3000000 --clicks 750000 --output /out/seed7
docker run --rm --cpus 4 -v "$DATA_ROOT_ABS:/out" rust:1.85 \
  /out/generate --seed 11 --events 5000000 --clicks 1250000 --output /out/seed11
docker run --rm --cpus 6 -v "$DATA_ROOT_ABS:/out" rust:1.85 \
  /out/generate --seed 19 --events 20000000 --clicks 5000000 --output /out/big19

for seed in seed7 seed11; do
  "$PY" reference/reference.py "$DATA_ROOT/$seed" "$DATA_ROOT/ref-$seed"
  bash scripts/docker_run.sh "$DATA_ROOT/$seed" "$DATA_ROOT/rust-$seed"
  "$PY" scripts/compare.py "$DATA_ROOT/rust-$seed" "$DATA_ROOT/ref-$seed"
done

time CPUS=8 MEM=24g bash scripts/docker_run.sh "$DATA_ROOT/big19" "$DATA_ROOT/rust-big19"
```

For the delimiter boundary test, run the same x86-64-v3 build target locally.

```sh
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo test --locked --release
```

## Local benchmark

The hypothesis was that scanning delimiters once with AVX2 would reduce CSV
parse time while preserving every field and malformed-line decision. The
baseline is `4ea215e` and the optimized parser is `c3fdb5c`. The fixed input
is seed 19 with 20M events and 5M clicks, run via the scorer launcher on Docker
Desktop with `CPUS=8 MEM=24g`. Wall time includes the launcher and Docker startup.

| Stage | Baseline | AVX2 |
| --- | ---: | ---: |
| Catalog and links | 0.846 s | 0.796 s |
| Event parse and partition | 3.875 s | 3.551 s |
| Event dedup and repartition | 0.826 s | 0.655 s |
| Click parse and partition | 0.768 s | 0.624 s |
| Sort, aggregate and write | 1.970 s | 1.260 s |
| Total process | 8.286 s | 6.887 s |
| Launcher wall time | 10.595 s | 8.819 s |

These measurements use local generated data and local hardware. The stop
condition was 36 passing fixtures, exact matches against the official dev answer
and both independent reference runs, and a measured 20M-event run.

The repeat pair ran the baseline followed by the final image against the same
seed. Baseline wall and process times were 8.765 s and 6.499 s. The final
image took 8.225 s wall and 6.520 s in-process. The timings include run-to-run
variation, especially for the first baseline run.

## Verification results

| Input | User rows | Campaign rows | Result |
| --- | ---: | ---: | --- |
| Fixtures | | | `36/36 fixtures passed` |
| Dev seed 1 vs. official answer | 117,038 | 27,400 | `MATCH` |
| Seed 7 vs. Python reference | 346,988 | 28,008 | `MATCH` |
| Seed 11 vs. Python reference | 571,034 | 28,014 | `MATCH` |
| Seed 19 final image vs. baseline image | 2,236,911 | 28,014 | `MATCH` |
| AVX2 vs. scalar delimiter test | | | `1 passed; 0 failed` |

Each `compare.py` run reported zero rows unique to either side and zero
duplicate keys for both tables.

## Compact ID benchmark

The optimization uses reversible 64-bit keys for short ASCII identifiers.
Long strings, non-ASCII strings, and strings with a NUL byte use a separate
exact index. Link minima and tied click IDs still compare the original bytes.
The pipeline resolves links alongside event parsing, then applies the canonical
ID to each surviving event after deduplication. ISO timestamps use SIMD digit
decoding and a Gregorian month table covering every four-digit year.

Same generated seed 19 (20M events, 5M clicks), eight Docker vCPUs, local Linux
volume for input and tmpfs for output:

| Image | In-process time | Docker CLI wall time |
| --- | ---: | ---: |
| Prior AVX2 parser | 3.591 s | 4.439 s |
| Compact IDs, serial link setup | 1.460 s | 1.869 s |
| Compact IDs, overlapped link setup (best) | 1.113 s | 1.537 s |
| Compact IDs, overlapped link setup (another run) | 1.219 s | 1.630 s |

The overlapped build took 6.006 s wall time under the Windows bind-mount
launcher. It passed all 36 fixtures and matched the official dev answer,
the Python oracle for seeds 7 and 11, and the earlier baseline at 20M events.
Both Rust unit tests passed. Pre-sizing event buckets did not produce a
consistent total-runtime gain, so the branch keeps dynamic bucket capacities.

To reproduce the Linux-volume timing from Git Bash with the generated
`big19` data and the optimized Docker image:

```sh
DATA_ABS=$(cd ../ss-data/big19 && pwd -W)
MSYS_NO_PATHCONV=1 docker volume create ss-seed19-local
MSYS_NO_PATHCONV=1 docker run --rm -v "$DATA_ABS:/source:ro" \
  -v ss-seed19-local:/data debian:bookworm-slim cp -r /source/. /data/
time MSYS_NO_PATHCONV=1 docker run --rm --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
  --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
  --tmpfs /tmp:rw,size=64m,mode=1777 --tmpfs /output:rw,size=128m,mode=1777 \
  -v ss-seed19-local:/data:ro shopping-sessions:latest
```
