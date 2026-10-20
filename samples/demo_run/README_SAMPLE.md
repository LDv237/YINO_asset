# TickForge offline sample

Truncated from `artifacts/runs/20261020T072820Z-53b5`: the first 20,000 WAL
records (64.884 s of the original run).

Replay and verify it offline (no exchange access required):

```bash
python -m tickforge replay --run samples/demo_run --verify
```

Expected result: `deterministic=true`, `verified=true`, all three digests match.
The `summary.json` in this directory is an **offline reference** generated from
this sample by `tools/make_sample_reference.py`; it is not a live exchange run.
The live-vs-replay proof for the 15-minute run is in
`artifacts/runs/20261020T072820Z-53b5/replay/replay_summary.json`.

Human-readable raw events (one JSON object per line):

```text
raw_events.jsonl.gz   20,000 records exported from the WAL
```

Quick-view files (plain text, no gzip tool needed):

```text
raw_events_head.jsonl        first 100 JSONL lines
raw_events_head.pretty.json  first 3 records, pretty-printed
```

CSV views (Excel-friendly):

```text
csv/agg_trades.csv      1,074 rows
csv/book_ticker.csv     16,021 rows
csv/depth_updates.csv   2,899 rows
csv/snapshots.csv       6 rows
```

Regenerate the sample / reference summary:

```bash
python tools/trim_run.py --run artifacts/runs/20261020T072820Z-53b5 \
  --out samples/demo_run --records 20000
python tools/make_sample_reference.py --sample samples/demo_run
```
