"""
validate.py  (Member 2 - Validation)

Plain-pandas data-quality checks (no extra dependencies). Each check is recorded
in a ValidationReport with a severity:

  error : the table cannot be used as-is (e.g. schema drift) -> table is skipped
  warn  : rows were quarantined / flagged but the load can continue
  info  : worth knowing, nothing to fix
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

RAW_SCHEMAS: dict[str, list[str]] = {
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                  "customer_city", "customer_state"],
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
               "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id",
                    "shipping_limit_date", "price", "freight_value"],
    "products": ["product_id", "product_category_name", "product_name_lenght",
                 "product_description_lenght", "product_photos_qty", "product_weight_g",
                 "product_length_cm", "product_height_cm", "product_width_cm"],
}

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "products": ["product_id"],
}

VALID_ORDER_STATUS = {"delivered", "shipped", "canceled", "unavailable", "invoiced",
                      "processing", "created", "approved"}


@dataclass
class Check:
    table: str
    name: str
    severity: str      # error | warn | info
    passed: bool
    bad_rows: int = 0
    detail: str = ""


class ValidationReport:
    def __init__(self) -> None:
        self.checks: list[Check] = []

    def add(self, table: str, name: str, bad_rows: int, severity: str = "warn", detail: str = "") -> None:
        self.checks.append(Check(table, name, severity, bad_rows == 0, int(bad_rows), detail))

    @property
    def errors(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "error"]

    @property
    def warnings(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "warn"]

    def to_dict(self) -> dict:
        return {
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "checks": [asdict(c) for c in self.checks],
        }

    def print_summary(self) -> None:
        for c in self.checks:
            if c.passed:
                continue
            print(f"  [{c.severity.upper():5}] {c.table}.{c.name}: {c.bad_rows} rows {c.detail}".rstrip())
        print(f"  -> {len(self.checks)} checks, {len(self.errors)} errors, {len(self.warnings)} warnings")


# ----------------------------------------------------------------------------- raw
def check_schema(table: str, df: pd.DataFrame, report: ValidationReport) -> bool:
    expected = set(RAW_SCHEMAS[table])
    actual = set(df.columns)
    missing, extra = sorted(expected - actual), sorted(actual - expected)
    report.add(table, "schema_missing_columns", len(missing), "error",
               f"missing={missing}" if missing else "")
    report.add(table, "schema_unexpected_columns", len(extra), "warn",
               f"extra={extra}" if extra else "")
    return not missing


def validate_raw(tables: dict[str, pd.DataFrame], report: ValidationReport) -> set[str]:
    """Run raw-level checks. Returns the set of table names that are safe to transform."""
    usable: set[str] = set()
    for name, df in tables.items():
        if not check_schema(name, df, report):
            continue
        usable.add(name)
        keys = PRIMARY_KEYS[name]
        report.add(name, "empty_table", int(df.empty), "warn")
        report.add(name, "null_primary_key", int(df[keys].isna().any(axis=1).sum()))
        report.add(name, "duplicate_primary_key", int(df.duplicated(keys).sum()))

        if name == "order_items":
            price = pd.to_numeric(df["price"], errors="coerce")
            freight = pd.to_numeric(df["freight_value"], errors="coerce")
            report.add(name, "price_not_positive", int((price.isna() | (price <= 0)).sum()))
            report.add(name, "freight_negative", int((freight.isna() | (freight < 0)).sum()))
        elif name == "orders":
            status = df["order_status"].astype("string").str.strip().str.lower()
            report.add(name, "unknown_order_status", int((~status.isin(VALID_ORDER_STATUS)).sum()),
                       detail=str(sorted(set(status.dropna()) - VALID_ORDER_STATUS) or ""))
            ts = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
            report.add(name, "bad_purchase_timestamp", int(ts.isna().sum()))
        elif name == "products":
            for c in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
                v = pd.to_numeric(df[c], errors="coerce")
                report.add(name, f"{c}_missing_or_not_positive", int((v.isna() | (v <= 0)).sum()))
        elif name == "customers":
            st = df["customer_state"].astype("string").str.strip()
            report.add(name, "bad_state_code", int((st.str.len() != 2).fillna(True).sum()))
    return usable


# ----------------------------------------------------------------------------- clean
def validate_clean(clean: dict[str, pd.DataFrame], report: ValidationReport) -> None:
    """Business-logic checks on the transformed tables."""
    orders = clean.get("fact_orders")
    items = clean.get("fact_order_items")
    products = clean.get("dim_products")

    if orders is not None:
        ts = orders
        report.add("fact_orders", "approved_before_purchase",
                   int((ts["order_approved_at"] < ts["order_purchase_timestamp"]).sum()))
        report.add("fact_orders", "delivered_before_carrier_pickup",
                   int((ts["order_delivered_customer_date"] < ts["order_delivered_carrier_date"]).sum()))
        report.add("fact_orders", "delivered_status_without_delivery_date",
                   int(((ts["order_status"] == "delivered") & ts["order_delivered_customer_date"].isna()).sum()))
        report.add("fact_orders", "delivery_date_but_not_delivered_status",
                   int(((ts["order_status"] != "delivered") & ts["order_delivered_customer_date"].notna()).sum()))
        report.add("fact_orders", "orders_without_items", int((~ts["has_items"]).sum()), "info")
        if items is not None:
            diff = abs(orders["items_value"].sum() - items["price"].sum())
            report.add("fact_orders", "items_value_matches_items_total", int(diff > 0.01), "error",
                       f"diff={diff:.2f}")

    if items is not None:
        report.add("fact_order_items", "shipping_limit_before_year_2016",
                   int((items["shipping_limit_date"] < "2016-01-01").sum()))

    if products is not None:
        report.add("dim_products", "category_missing", int((products["category_status"] == "missing").sum()),
                   "warn", "category set to 'unknown'")
        report.add("dim_products", "category_untranslated",
                   int((products["category_status"] == "untranslated").sum()),
                   "warn", "no English name; Portuguese name kept")
        report.add("dim_products", "missing_dimensions", int(products["has_missing_dimensions"].sum()),
                   "info", "kept, flagged has_missing_dimensions")
