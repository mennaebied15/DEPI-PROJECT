"""
run_pipeline.py  (Member 2)

gathered_data/ (from the extractor)  ->  processed_data/ (clean tables + reports)

Usage:
    python run_pipeline.py --date 2026-10-24
    python run_pipeline.py --date 2026-10-24 --input-dir gathered_data --out-dir processed_data

Exit codes: 0 = all good, 2 = finished but some table(s) were skipped (see report), 1 = nothing to process.
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

import incremental as inc
import transform as tf
import validate as v

log = logging.getLogger("transform")

RAW_TABLES = ["customers", "orders", "order_items", "products"]
# output table -> primary key
KEYS = {
    "dim_customers": ["customer_id"],
    "dim_products": ["product_id"],
    "fact_orders": ["order_id"],
    "fact_order_items": ["order_id", "order_item_id"],
}


def read_raw(input_dir: Path, date_str: str) -> dict[str, pd.DataFrame]:
    raw = {}
    for t in RAW_TABLES:
        p = input_dir / f"gathered_{t}_data_{date_str}.csv"
        if p.exists():
            raw[t] = pd.read_csv(p, dtype="string")  # read everything as text; types are set in transform
        else:
            log.warning("missing input file: %s", p)
    return raw


def read_translation(input_dir: Path) -> pd.DataFrame | None:
    p = input_dir / "olist_category_translation.csv"
    if p.exists():
        return pd.read_csv(p, dtype="string")
    log.warning("category translation file not found at %s (English names will fall back to Portuguese)", p)
    return None


def known_ids(current: dict[str, pd.DataFrame | None], new: pd.DataFrame | None, key: str, table: str) -> set:
    ids = set(new[key].dropna().astype(str)) if new is not None else set()
    cur = current.get(table)
    if cur is not None:
        ids |= set(cur[key].dropna().astype(str))
    return ids


def run(date_str: str, input_dir: Path, out_dir: Path) -> int:
    datetime.strptime(date_str, "%Y-%m-%d")  # fail early on a bad date
    cur_dir, run_dir = out_dir / "current", out_dir / "runs" / date_str
    run_dir.mkdir(parents=True, exist_ok=True)
    cur_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / "state" / "watermark.json"
    state = inc.load_state(state_path)

    raw = read_raw(input_dir, date_str)
    if not raw:
        log.error("no input files for %s in %s", date_str, input_dir)
        return 1

    report = v.ValidationReport()
    usable = v.validate_raw(raw, report)
    skipped = sorted(set(RAW_TABLES) - usable)
    for t in skipped:
        log.error("table '%s' skipped (missing file or failed schema check)", t)

    # ---- transform --------------------------------------------------------
    rej: dict[str, list] = {t: [] for t in RAW_TABLES}
    translation = read_translation(input_dir)
    out: dict[str, pd.DataFrame] = {}
    if "products" in usable:
        out["dim_products"] = tf.clean_products(raw["products"], translation, rej["products"])
    if "customers" in usable:
        out["dim_customers"] = tf.clean_customers(raw["customers"], rej["customers"])
    orders = tf.clean_orders(raw["orders"], rej["orders"]) if "orders" in usable else None
    items = tf.clean_order_items(raw["order_items"], rej["order_items"]) if "order_items" in usable else None

    # ---- foreign-key guard (against what is already loaded + this batch) ---
    current = {t: inc.load_current(cur_dir / f"{t}.csv") for t in KEYS}
    if orders is not None and "dim_customers" in out:
        orders = tf.reject_unknown_keys(
            orders, "customer_id", known_ids(current, out["dim_customers"], "customer_id", "dim_customers"),
            "orphan_customer", rej["orders"])
    elif orders is not None:
        log.warning("customers unavailable: orders->customers foreign key NOT checked")
    if items is not None and orders is not None:
        items = tf.reject_unknown_keys(
            items, "order_id", known_ids(current, orders, "order_id", "fact_orders"),
            "orphan_order", rej["order_items"])
    if items is not None and "dim_products" in out:
        items = tf.reject_unknown_keys(
            items, "product_id", known_ids(current, out["dim_products"], "product_id", "dim_products"),
            "orphan_product", rej["order_items"])
    if items is not None:
        out["fact_order_items"] = items
    if orders is not None and items is not None:
        out["fact_orders"] = tf.build_fact_orders(orders, items)
    elif orders is not None:
        log.warning("order_items unavailable: fact_orders not built")

    v.validate_clean(out, report)

    # ---- incremental load -------------------------------------------------
    stats = {}
    for name, df in out.items():
        res = inc.upsert(current[name], df, KEYS[name])
        res.merged.to_csv(cur_dir / f"{name}.csv", index=False)
        res.delta.to_csv(run_dir / f"delta_{name}.csv", index=False)
        stats[name] = {"inserted": res.inserted, "updated": res.updated,
                       "unchanged": res.unchanged, "total_after": len(res.merged)}
        log.info("%-17s +%d new, %d updated, %d unchanged -> %d total",
                 name, res.inserted, res.updated, res.unchanged, len(res.merged))

    note = None
    if "fact_orders" in out and len(out["fact_orders"]):
        fo = out["fact_orders"]
        note = inc.watermark_note(state, fo["order_purchase_timestamp"].min())
        if note:
            report.add("fact_orders", "batch_overlaps_watermark", 1, "info", note)
        batch_max = fo["order_purchase_timestamp"].max()
        wm = state.get("watermark")
        state["watermark"] = str(max(pd.Timestamp(wm), batch_max)) if wm else str(batch_max)

    # ---- rejected rows + report ------------------------------------------
    rejected_counts = {}
    for t, parts in rej.items():
        if parts:
            r = pd.concat(parts, ignore_index=True)
            r.to_csv(run_dir / f"rejected_{t}.csv", index=False)
            rejected_counts[t] = r["reject_reason"].value_counts().to_dict()
    summary = {
        "run_date": date_str,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "tables_skipped": skipped,
        "load_stats": stats,
        "rejected_rows": rejected_counts,
        "watermark_after": state["watermark"],
        "validation": report.to_dict(),
    }
    (run_dir / "validation_report.json").write_text(json.dumps(summary, indent=2, default=str))
    state["runs"].append({k: summary[k] for k in ("run_date", "finished_at", "load_stats", "tables_skipped")})
    inc.save_state(state, state_path)

    print(f"\nRun {date_str}: validation summary")
    report.print_summary()
    if rejected_counts:
        print("  rejected rows:", rejected_counts)
    return 2 if skipped else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD batch date")
    ap.add_argument("--input-dir", default="gathered_data")
    ap.add_argument("--out-dir", default="processed_data")
    a = ap.parse_args()
    raise SystemExit(run(a.date, Path(a.input_dir), Path(a.out_dir)))
