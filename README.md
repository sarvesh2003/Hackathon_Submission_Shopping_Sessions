# Shopping Sessions: Builders Buildathon, Databricks track

Turns the messy click-stream CSVs in `/data` into two exact daily Parquet tables (`user_daily`, `campaign_daily`), as fast as possible.

- Rules: [docs/rules.md](docs/rules.md). Track facts: [docs/track-page.md](docs/track-page.md).
- Plan and status: [PLAN.md](PLAN.md), [PROGRESS.md](PROGRESS.md). Instructions for Claude Code: [CLAUDE.md](CLAUDE.md).

## Build and test
```sh
make kit                 # official generator and dev answer into kit/
make data                # local datasets into data/
make fixtures            # 36 edge-case fixtures, Docker image with the scorer's flags
make run-dev check-dev   # public dev set vs the official answer
make check20             # fast image vs the simple Rust reference on 20M events
make bench20             # median of 3 timed runs on 20M events
```

## Third-party material
- Official Buildathon starter material: `fixtures/`, `scripts/run_fixtures.py`, `scripts/compare.py`, `scripts/docker_run.sh`, `docs/rules.md`.
- Rust crates: listed here as they are added (see `Cargo.toml`).
- Python (official test scripts only): polars, pyarrow.

## License
MIT. See [LICENSE](LICENSE).
