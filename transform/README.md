# Transformation & Validation (Member 2)

Turns the raw files from the extractor (`gathered_data/`) into clean, typed,
analytics-ready tables in `processed_data/`, validates them, and loads them
incrementally.

```
gathered_data/*.csv  ->  validate raw  ->  transform  ->  validate clean  ->  upsert  ->  processed_data/
```

## Run

```bash
pip install -r requirements.txt
python run_pipeline.py --date 2026-10-24            # one dated batch
python run_pipeline.py --date 2026-10-24 --input-dir gathered_data --out-dir processed_data
```

Exit code `0` = OK, `2` = finished but at least one table was skipped, `1` = no input.
Airflow can use the exit code directly (non-zero => task fails/retries).

## Input (from the extractor)

`gathered_data/gathered_{customers,orders,order_items,products}_data_<date>.csv`
plus `gathered_data/olist_category_translation.csv` (one file, not dated).

## Output (`processed_data/`)

| Path | What |
|---|---|
| `current/<table>.csv` | Cumulative clean tables (what the DB engineer loads) |
| `runs/<date>/delta_<table>.csv` | Only rows inserted/updated by this run |
| `runs/<date>/rejected_<table>.csv` | Quarantined rows with `reject_reason` |
| `runs/<date>/validation_report.json` | Every check, row counts, load stats |
| `state/watermark.json` | Watermark (max purchase timestamp) + run history |

Tables and primary keys:

| Table | PK | Notes |
|---|---|---|
| `dim_products` | `product_id` | typo columns fixed (`*_length`), English category, `volume_cm3`, flags |
| `dim_customers` | `customer_id` | zip padded to 5 digits, city/state normalised |
| `fact_orders` | `order_id` | parsed timestamps, `purchase_date_key` (YYYYMMDD, joins to Dim Date), `delivery_days`, `delivery_delay_days`, `is_late`, item totals, `order_total` |
| `fact_order_items` | `order_id`, `order_item_id` | `item_total = price + freight_value` |

## Cleaning rules

- Strip whitespace, empty string -> NULL, lowercase `order_status`.
- Timestamps -> datetime (`errors="coerce"`), numbers -> numeric, counts -> nullable Int.
- Duplicate primary keys: keep the last row, log the rest as rejected.
- `price <= 0`, negative freight, NULL keys: rejected (not silently dropped).
- Product weight/size `<= 0` -> NULL (flag `has_missing_dimensions`). Missing category -> `unknown`.
- `pc_gamer` is missing from Olist's translation file; mapped manually (`CATEGORY_FALLBACK`).
- Foreign-key guard: items whose order/product is not in the cumulative tables, and orders whose customer is unknown, are rejected so the DB load with FKs enabled cannot fail.

## Validation (`validate.py`)

Severity: `error` (table skipped), `warn` (flagged/quarantined), `info`.
Covers schema drift (missing/unexpected columns), null/duplicate keys, ranges, allowed statuses,
date ordering, status vs. delivery-date consistency, category translation coverage,
and a reconciliation of `fact_orders.items_value` against `fact_order_items.price`.

## Incremental logic (`incremental.py`)

- Upsert by primary key: new keys are inserted, changed rows replace the old version, identical rows are untouched. Re-running a date is idempotent.
- Watermark = max `order_purchase_timestamp` loaded. A batch starting at/before it is reported as a replay/late-data note (still handled safely by the upsert).
- Each run writes a delta so the database can upsert only what changed.

## Known issues in the current data

1. `olist_customers_dataset_2026-10-24.csv` in the batched-data repo is an exact copy of
   `olist_order_items_dataset_2026-10-24.csv` (identical checksum). The pipeline flags it as a
   schema error and skips `dim_customers`. It must be regenerated upstream.
2. The extractor (`API.py`) does not download `olist_category_translation.csv`; add it
   (it is not dated) so English category names are populated.
