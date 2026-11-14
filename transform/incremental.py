"""
incremental.py  (Member 2 - Incremental loading)

Strategy
--------
* Each run processes ONE dated batch (same convention as the extractor).
* Dimensions and facts are UPSERTED by primary key into cumulative tables, so
  re-running the same batch is idempotent and a changed row (e.g. an order whose
  status moved from "shipped" to "delivered") replaces the old version.
* A WATERMARK (max order_purchase_timestamp loaded so far) is kept in a small
  JSON state file together with a log of runs. It is used to flag batches that
  overlap already-loaded history (replays / late data) and to tell the DB
  engineer which time range was covered.
* Every run also writes a DELTA (rows inserted + updated) so the database load
  can upsert only what changed.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class UpsertResult:
    merged: pd.DataFrame
    delta: pd.DataFrame
    inserted: int
    updated: int
    unchanged: int


def load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {"watermark": None, "runs": []}


def save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    os.replace(tmp, path)  # atomic: never leaves a half-written state file


def load_current(path: Path) -> pd.DataFrame | None:
    return pd.read_csv(path, dtype="string") if path.exists() else None


def _index(df: pd.DataFrame, keys: list[str]) -> pd.MultiIndex:
    return pd.MultiIndex.from_frame(df[keys].astype("string"))


def upsert(existing: pd.DataFrame | None, new: pd.DataFrame, keys: list[str]) -> UpsertResult:
    """Insert new keys, replace changed keys, leave identical rows alone."""
    if existing is None or existing.empty:
        return UpsertResult(new.copy(), new.copy(), len(new), 0, 0)

    new_s = new.astype("string")          # compare as text: CSV round-trips lose dtypes
    old_s = existing.astype("string")
    new_s.index = _index(new, keys)
    old_s.index = _index(existing, keys)
    cols = [c for c in new_s.columns if c in old_s.columns]

    is_new = ~new_s.index.isin(old_s.index)
    common = new_s.index[~is_new]
    a, b = new_s.loc[common, cols], old_s.loc[common, cols]
    a, b = a[~a.index.duplicated()], b[~b.index.duplicated()]
    same = ((a == b) | (a.isna() & b.isna())).all(axis=1)
    changed_idx = same.index[~same]

    inserted_mask = is_new
    changed_mask = new_s.index.isin(changed_idx)
    delta = new[inserted_mask | changed_mask]

    keep_old = existing[~old_s.index.isin(new_s.index)]
    merged = pd.concat([keep_old, new], ignore_index=True)
    return UpsertResult(merged, delta, int(inserted_mask.sum()), int(changed_mask.sum()),
                        int(same.sum()))


def watermark_note(state: dict, batch_min_ts: pd.Timestamp | None) -> str | None:
    """Return a note if this batch starts at/before the stored watermark (replay or late data)."""
    wm = state.get("watermark")
    if wm and batch_min_ts is not None and pd.Timestamp(batch_min_ts) <= pd.Timestamp(wm):
        return f"batch starts at {batch_min_ts} <= watermark {wm}: replay or late-arriving data (handled by upsert)"
    return None
