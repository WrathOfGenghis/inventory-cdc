#!/usr/bin/env python3
"""Aggregate per-run benchmark JSON files into a single summary.

Reads every *.json file under the supplied results directory, groups by
scenario, and prints mean / std-dev for the rate and event counts. The
output mirrors the tables in §16 of the design report.

Example:
    python load-test/benchmark.py --aggregate results/ --out figures/
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path


def load_runs(results_dir: Path) -> list[dict]:
    runs: list[dict] = []
    for path in sorted(results_dir.glob("*.json")):
        try:
            runs.append(json.loads(path.read_text()))
        except json.JSONDecodeError as exc:
            print(f"skip {path}: {exc}")
    return runs


def aggregate(runs: list[dict]) -> dict:
    by_scenario: dict[str, list[dict]] = {}
    for r in runs:
        by_scenario.setdefault(r.get("scenario", "unknown"), []).append(r)

    out: dict[str, dict] = {}
    for scenario, items in by_scenario.items():
        rates = [r["effective_rate"] for r in items if "effective_rate" in r]
        sent = [r["events_sent"] for r in items if "events_sent" in r]
        if not rates:
            continue
        out[scenario] = {
            "runs": len(items),
            "rate_mean": round(statistics.fmean(rates), 2),
            "rate_stdev": round(statistics.pstdev(rates), 2) if len(rates) > 1 else 0.0,
            "events_sent_mean": round(statistics.fmean(sent), 2),
            "events_sent_total": sum(sent),
        }
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--aggregate", required=True, type=Path,
                   help="directory containing per-run *.json files")
    p.add_argument("--out", type=Path, default=None,
                   help="optional directory to write the aggregate summary into")
    args = p.parse_args()

    if not args.aggregate.is_dir():
        raise SystemExit(f"not a directory: {args.aggregate}")

    runs = load_runs(args.aggregate)
    if not runs:
        print("no runs found")
        return 1

    summary = aggregate(runs)
    print(json.dumps(summary, indent=2))

    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "summary.json").write_text(json.dumps(summary, indent=2))
        print(f"wrote {args.out / 'summary.json'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
