"""Read raw source files *as strings* and stamp lineage columns.

Nothing is cleaned here: bronze must be a faithful, replayable copy of the
source so any silver/gold row can be re-derived (backfill) from it.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

ARTICLE_COLUMNS = (
    "article_id", "title", "company_name", "published_date", "category",
    "revenue", "summary", "url", "author", "word_count",
)
# Known upstream renames -> canonical bronze column (schema-change shim).
COLUMN_RENAMES: dict[str, str] = {
    "id": "article_id",
    "company": "company_name",
    "date": "published_date",
    "publish_date": "published_date",
    "arr": "revenue",
}
METADATA_COLUMNS = (
    "founded_year", "headquarters", "employee_count", "industry", "is_public", "stock_ticker",
)


@dataclass(frozen=True)
class Batch:
    batch_id: str
    source_type: str  # "articles" | "company_metadata"
    source_file: str
    file_sha256: str
    frame: pd.DataFrame

    @property
    def row_count(self) -> int:
        return len(self.frame)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def record_hash(values: list[Any]) -> str:
    """Content hash of a source record; identifies a *version* of a record."""
    payload = "\x1f".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _batch_id(path: Path, sha: str) -> str:
    # Deterministic: re-delivering the same file yields the same batch_id.
    return f"{path.stem}-{sha[:12]}"


def read_articles_batch(path: Path) -> Batch:
    path = Path(path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    df = df.rename(columns={k: v for k, v in COLUMN_RENAMES.items() if k in df.columns and v not in df.columns})
    missing = [c for c in ARTICLE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing required columns {missing}")
    extra = [c for c in df.columns if c not in ARTICLE_COLUMNS]

    sha = file_sha256(path)
    out = df[list(ARTICLE_COLUMNS)].copy()
    for col in ARTICLE_COLUMNS:
        out[col] = out[col].astype(str).str.strip()
    # Unknown/new source columns are preserved (schema drift is visible, not dropped).
    out["extra_columns_json"] = (
        df[extra].apply(lambda r: json.dumps(r.to_dict(), sort_keys=True), axis=1) if extra else None
    )
    out["source_row_number"] = range(1, len(out) + 1)
    out["record_hash"] = [record_hash(list(r)) for r in out[list(ARTICLE_COLUMNS)].itertuples(index=False)]
    # A record without an id cannot be upserted; give it a deterministic placeholder and let DQ flag it.
    no_id = out["article_id"] == ""
    out.loc[no_id, "article_id"] = "MISSING-" + out.loc[no_id, "record_hash"]
    out["batch_id"] = _batch_id(path, sha)
    out["source_file"] = path.name
    return Batch(_batch_id(path, sha), "articles", path.name, sha, out)


def read_metadata_batch(path: Path) -> Batch:
    path = Path(path)
    with open(path, encoding="utf-8") as fh:
        payload = json.load(fh)
    if isinstance(payload, list):  # tolerate a list-of-objects layout too
        payload = {item["company_name"]: {k: v for k, v in item.items() if k != "company_name"} for item in payload}

    sha = file_sha256(path)
    rows = []
    for i, (name, attrs) in enumerate(payload.items(), start=1):
        raw_json = json.dumps(attrs, sort_keys=True)
        row = {"company_name": str(name).strip()}
        for col in METADATA_COLUMNS:
            v = attrs.get(col)
            row[col] = None if v is None else (json.dumps(v) if isinstance(v, bool) else str(v))
        row["raw_json"] = raw_json
        row["source_row_number"] = i
        row["record_hash"] = record_hash([row["company_name"], raw_json])
        rows.append(row)
    out = pd.DataFrame(rows, columns=["company_name", *METADATA_COLUMNS, "raw_json", "source_row_number", "record_hash"])
    out["batch_id"] = _batch_id(path, sha)
    out["source_file"] = path.name
    return Batch(_batch_id(path, sha), "company_metadata", path.name, sha, out)
