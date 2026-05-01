#!/usr/bin/env python3
"""Synthetic CDC event generator for the inventory pipeline.

Drives realistic warehouse mutations against the local Postgres so Debezium
captures and publishes them, exercising the full pipeline. Three scenarios
are supported:

  * sustained : steady rate for a fixed duration (used for §16.1 numbers)
  * burst     : N events as fast as possible (the 100k correction scenario)
  * soak      : long-duration low-rate test for memory leak detection

Example:
    python load-test/load_gen.py --scenario sustained --rate 800 --duration 600
    python load-test/load_gen.py --scenario burst --total 100000

Requires: psycopg2-binary >= 2.9
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

try:
    import psycopg2
except ImportError:  # pragma: no cover
    print("psycopg2 is required: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

DEFAULT_DSN = "postgres://postgres:postgres@localhost:5432/warehouse"

# Small SKU catalogue mirroring the seed in 0004_seed_data.sql plus a few
# extras so the workload exercises more than the seed rows.
PRODUCT_IDS = [
    "SKU-AC-9482", "SKU-BG-3310", "SKU-CD-7001", "SKU-DE-4422",
    "SKU-EF-5510", "SKU-FG-1107", "SKU-GH-8821", "SKU-IJ-6633",
]
WAREHOUSES = ["WH-MUM-01", "WH-DEL-02", "WH-BLR-03"]

# Weighted delta distribution: more decrements (sales) than increments
# (restocks), which approximates a real e-commerce workload.
DELTAS = [-1, -1, -1, -2, -3, +5, +10, +20]


@dataclass
class Stats:
    """Per-run counters used by the report writer."""
    sent: int = 0
    failed: int = 0
    started_at: float = field(default_factory=time.time)

    def to_dict(self, scenario: str) -> dict:
        elapsed = time.time() - self.started_at
        return {
            "scenario": scenario,
            "events_sent": self.sent,
            "events_failed": self.failed,
            "elapsed_seconds": round(elapsed, 3),
            "effective_rate": round(self.sent / elapsed, 2) if elapsed else 0,
        }


def connect(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def mutate_one(cur) -> None:
    """Issue one randomised UPDATE against warehouse_inventory."""
    product_id = random.choice(PRODUCT_IDS)
    warehouse_id = random.choice(WAREHOUSES)
    delta = random.choice(DELTAS)

    cur.execute(
        """
        UPDATE warehouse_inventory
           SET available_qty = GREATEST(0, available_qty + %s),
               row_version   = row_version + 1,
               last_updated  = NOW()
         WHERE product_id   = %s
           AND warehouse_id = %s
        """,
        (delta, product_id, warehouse_id),
    )
    if cur.rowcount == 0:
        # Row not in seed; insert it so the workload spreads out.
        cur.execute(
            """
            INSERT INTO warehouse_inventory
                (product_id, warehouse_id, available_qty, reserved_qty,
                 stock_status, row_version)
            VALUES (%s, %s, %s, 0, 'ACTIVE', 1)
            ON CONFLICT (product_id, warehouse_id) DO NOTHING
            """,
            (product_id, warehouse_id, max(0, 50 + delta)),
        )


def run_sustained(conn, rate: int, duration: int, stats: Stats) -> None:
    interval = 1.0 / max(rate, 1)
    deadline = time.time() + duration
    cur = conn.cursor()
    next_tick = time.time()

    while time.time() < deadline:
        try:
            mutate_one(cur)
            conn.commit()
            stats.sent += 1
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            stats.failed += 1
            print(f"warn: {exc}", file=sys.stderr)

        next_tick += interval
        sleep_for = next_tick - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)


def run_burst(conn, total: int, stats: Stats) -> None:
    cur = conn.cursor()
    batch_size = 500

    for chunk_start in range(0, total, batch_size):
        chunk_end = min(chunk_start + batch_size, total)
        try:
            for _ in range(chunk_end - chunk_start):
                mutate_one(cur)
            conn.commit()
            stats.sent += chunk_end - chunk_start
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            stats.failed += chunk_end - chunk_start
            print(f"warn: {exc}", file=sys.stderr)


def run_soak(conn, rate: int, duration: int, stats: Stats) -> None:
    # Soak is just a low-rate sustained run with periodic progress printing.
    last_print = time.time()
    interval = 1.0 / max(rate, 1)
    deadline = time.time() + duration
    cur = conn.cursor()

    while time.time() < deadline:
        try:
            mutate_one(cur)
            conn.commit()
            stats.sent += 1
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            stats.failed += 1

        now = time.time()
        if now - last_print > 60:
            print(
                f"[soak] elapsed={int(now - stats.started_at)}s "
                f"sent={stats.sent} failed={stats.failed}"
            )
            last_print = now
        time.sleep(interval)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", choices=["sustained", "burst", "soak"], required=True)
    p.add_argument("--rate", type=int, default=100, help="events per second")
    p.add_argument("--duration", type=int, default=60, help="seconds (sustained, soak)")
    p.add_argument("--total", type=int, default=100_000, help="total events (burst)")
    p.add_argument("--dsn", default=DEFAULT_DSN)
    p.add_argument("--report", type=Path, default=None, help="write JSON summary here")
    args = p.parse_args()

    random.seed()  # entropy from OS

    stats = Stats()
    with connect(args.dsn) as conn:
        if args.scenario == "sustained":
            run_sustained(conn, args.rate, args.duration, stats)
        elif args.scenario == "burst":
            run_burst(conn, args.total, stats)
        else:
            run_soak(conn, args.rate, args.duration, stats)

    summary = stats.to_dict(args.scenario)
    print(json.dumps(summary, indent=2))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(summary, indent=2))

    return 0 if stats.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
