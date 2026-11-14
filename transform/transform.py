"""
transform.py  (Member 2 - Transformation)

Raw gathered Olist CSVs  ->  clean, typed, analytics-ready tables.

Every clean_* function returns the clean DataFrame and appends any rows it had
to throw out to the `rejected` list (with a `reject_reason` column), so nothing
disappears silently.
"""
from __future__ import annotations

import pandas as pd

ORDER_TS_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

# Categories that exist in products but are missing from Olist's translation file.
CATEGORY_FALLBACK = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_food_preparers",
}


# --------------------------------------------------------------------------- helpers
def _strip(s: pd.Series) -> pd.Series:
    """Trim whitespace; empty strings become NA."""
    out = s.astype("string").str.strip()
    return out.mask(out == "", pd.NA)


def _reject(df: pd.DataFrame, bad: pd.Series, reason: str, rejected: list) -> pd.DataFrame:
    """Move rows where `bad` is True into `rejected`; return the rest."""
    bad = bad.fillna(False).astype(bool)
    if bad.any():
        r = df[bad].copy()
        r["reject_reason"] = reason
        rejected.append(r)
    return df[~bad]


def _dedupe(df: pd.DataFrame, keys: list[str], reason: str, rejected: list) -> pd.DataFrame:
    """Keep the last occurrence of each key; log the earlier ones as rejected."""
    dup = df.duplicated(keys, keep="last")
    return _reject(df, dup, reason, rejected)


def _days(delta: pd.Series) -> pd.Series:
    return (delta.dt.total_seconds() / 86400).round(2)


# --------------------------------------------------------------------------- tables
def clean_orders(raw: pd.DataFrame, rejected: list) -> pd.DataFrame:
    df = raw.copy()
    for c in ["order_id", "customer_id", "order_status"]:
        df[c] = _strip(df[c])
    df["order_status"] = df["order_status"].str.lower()

    df = _reject(df, df["order_id"].isna() | df["customer_id"].isna(), "null_key", rejected)
    for c in ORDER_TS_COLS:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    df = _reject(df, df["order_purchase_timestamp"].isna(), "bad_purchase_timestamp", rejected)
    df = _dedupe(df, ["order_id"], "duplicate_order_id", rejected)

    p = df["order_purchase_timestamp"]
    df["purchase_date_key"] = (p.dt.year * 10000 + p.dt.month * 100 + p.dt.day).astype("Int64")
    df["approval_hours"] = ((df["order_approved_at"] - p).dt.total_seconds() / 3600).round(2)
    df["delivery_days"] = _days(df["order_delivered_customer_date"] - p)
    df["delivery_delay_days"] = _days(
        df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]
    )
    delivered = df["order_delivered_customer_date"].notna()
    df["is_late"] = (df["delivery_delay_days"] > 0).astype("boolean").where(delivered)
    return df.reset_index(drop=True)


def clean_order_items(raw: pd.DataFrame, rejected: list) -> pd.DataFrame:
    df = raw.copy()
    for c in ["order_id", "product_id", "seller_id"]:
        df[c] = _strip(df[c])
    df["order_item_id"] = pd.to_numeric(df["order_item_id"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")
    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")

    null_key = df[["order_id", "order_item_id", "product_id"]].isna().any(axis=1)
    df = _reject(df, null_key, "null_key", rejected)
    df = _reject(df, df["price"].isna() | (df["price"] <= 0), "invalid_price", rejected)
    df = _reject(df, df["freight_value"].isna() | (df["freight_value"] < 0), "invalid_freight", rejected)
    df = _dedupe(df, ["order_id", "order_item_id"], "duplicate_item", rejected)

    df["item_total"] = (df["price"] + df["freight_value"]).round(2)
    return df.reset_index(drop=True)


def clean_products(raw: pd.DataFrame, translation: pd.DataFrame | None, rejected: list) -> pd.DataFrame:
    df = raw.rename(columns={  # fix the typo in the source column names
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    }).copy()
    df["product_id"] = _strip(df["product_id"])
    df = _reject(df, df["product_id"].isna(), "null_key", rejected)
    df = _dedupe(df, ["product_id"], "duplicate_product_id", rejected)

    for c in ["product_name_length", "product_description_length", "product_photos_qty"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    dims = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    for c in dims:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # a weight/size of 0 or less is not real data: treat as missing rather than keep it
    for c in dims:
        df[c] = df[c].where(df[c] > 0)

    # category name + English translation
    cat = _strip(df["product_category_name"]).str.lower()
    tmap = dict(CATEGORY_FALLBACK)
    if translation is not None:
        tmap.update(dict(zip(
            _strip(translation["product_category_name"]).str.lower(),
            _strip(translation["product_category_name_english"]),
        )))
    english = cat.map(tmap)
    df["category_status"] = "ok"
    df.loc[cat.isna(), "category_status"] = "missing"
    df.loc[cat.notna() & english.isna(), "category_status"] = "untranslated"
    df["product_category_name"] = cat.fillna("unknown")
    df["product_category_name_english"] = english.fillna(cat).fillna("unknown")

    df["has_missing_dimensions"] = df[dims].isna().any(axis=1)
    df["volume_cm3"] = (df["product_length_cm"] * df["product_height_cm"] * df["product_width_cm"]).round(1)
    return df.reset_index(drop=True)


CUSTOMER_STATE_LEN = 2


def clean_customers(raw: pd.DataFrame, rejected: list) -> pd.DataFrame:
    df = raw.copy()
    for c in ["customer_id", "customer_unique_id", "customer_city", "customer_state"]:
        df[c] = _strip(df[c])
    df = _reject(df, df["customer_id"].isna(), "null_key", rejected)
    df = _dedupe(df, ["customer_id"], "duplicate_customer_id", rejected)
    df["customer_zip_code_prefix"] = (
        _strip(df["customer_zip_code_prefix"].astype("string")).str.zfill(5)
    )
    df["customer_city"] = df["customer_city"].str.lower().str.title()
    df["customer_state"] = df["customer_state"].str.upper()
    return df.reset_index(drop=True)


def build_fact_orders(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """One row per order, enriched with item-level totals (orders without items get 0)."""
    agg = items.groupby("order_id").agg(
        items_count=("order_item_id", "count"),
        distinct_products=("product_id", "nunique"),
        distinct_sellers=("seller_id", "nunique"),
        items_value=("price", "sum"),
        freight_value=("freight_value", "sum"),
    ).reset_index()
    f = orders.merge(agg, on="order_id", how="left")
    f["has_items"] = f["items_count"].notna()
    for c in ["items_count", "distinct_products", "distinct_sellers"]:
        f[c] = f[c].fillna(0).astype("Int64")
    for c in ["items_value", "freight_value"]:
        f[c] = f[c].fillna(0).round(2)
    f["order_total"] = (f["items_value"] + f["freight_value"]).round(2)
    return f


def reject_unknown_keys(df: pd.DataFrame, col: str, known: set, reason: str, rejected: list) -> pd.DataFrame:
    """Foreign-key guard: drop rows whose `col` isn't in the set of known parent keys."""
    return _reject(df, ~df[col].isin(known), reason, rejected)
