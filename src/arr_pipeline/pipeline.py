"""End-to-end orchestration: ingest -> silver -> gold -> embeddings -> checks -> exports."""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import duckdb
import pandas as pd

from . import warehouse
from .cleaning import CompanyResolver
from .config import (
    DEFAULT_ARTICLES_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_METADATA_PATH,
    EMBEDDINGS_DIR,
    OUTPUT_DIR,
    PARSER_VERSION,
    WAREHOUSE_CSV_DIR,
    AIExportFilter,
)
from .embeddings import Embedder, build_article_embeddings
from .ingest import read_articles_batch, read_metadata_batch
from .transform import build_company_resolution, clean_articles, clean_company_metadata

log = logging.getLogger("arr_pipeline")


@dataclass
class RunResult:
    run_id: str
    new_record_versions: int
    newly_embedded: int
    row_counts: dict[str, int]
    outputs: dict[str, Path] = field(default_factory=dict)


def build_silver(con: duckdb.DuckDBPyConnection, today: Optional[date] = None) -> None:
    """Re-derive every silver table from the current bronze versions."""
    bronze_articles, bronze_meta = warehouse.current_bronze(con)
    resolver = CompanyResolver(bronze_meta["company_name"].tolist())
    resolution = build_company_resolution(bronze_articles["company_name"].replace("", None), resolver)
    silver_articles, arr_parse = clean_articles(bronze_articles, resolution, today=today)
    company_meta = clean_company_metadata(bronze_meta, resolver)
    warehouse.load_silver(con, {
        "silver_company_resolution": resolution,
        "silver_company_metadata": company_meta,
        "silver_articles": silver_articles,
        "silver_arr_parse": arr_parse,
    })


def ai_articles_frame(con: duckdb.DuckDBPyConnection, spec: AIExportFilter = AIExportFilter()) -> pd.DataFrame:
    """The required downstream export: AI-related (category OR industry), 2022-2024, valid ARR > $50M."""
    has_emb = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'article_embeddings'").fetchone()[0]
    emb_cols = ("to_json(e.embedding) AS embedding, to_json(e.top_similar_articles) AS top_similar_articles"
                if has_emb else "NULL AS embedding, NULL AS top_similar_articles")
    emb_join = "LEFT JOIN article_embeddings e USING (article_id)" if has_emb else ""
    df = con.execute(f"""
        SELECT a.*, {emb_cols}
        FROM article_enriched a {emb_join}
        WHERE (a.category = ? OR a.industry = ?)
          AND a.published_year BETWEEN ? AND ?
          AND a.arr_status = 'valid' AND a.arr_usd > ?
        ORDER BY a.published_date, a.article_id
    """, [spec.ai_value, spec.ai_value, spec.min_year, spec.max_year, spec.min_arr_usd_exclusive]).df()
    return df[list(spec.columns)]


def run_pipeline(
    article_paths: Sequence[Path] = (DEFAULT_ARTICLES_PATH,),
    metadata_paths: Sequence[Path] = (DEFAULT_METADATA_PATH,),
    db_path: Optional[Path] = DEFAULT_DB_PATH,
    output_dir: Path = OUTPUT_DIR,
    embedder: Optional[Embedder] = None,
    full_refresh: bool = False,
    today: Optional[date] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> RunResult:
    """Run the whole pipeline. Safe to re-run: unchanged inputs produce identical modeled tables."""
    if full_refresh and db_path is not None and Path(db_path).exists():
        log.info("full refresh: removing %s", db_path)
        Path(db_path).unlink()
    own_con = con is None
    con = con or warehouse.connect(db_path)
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        # 1. bronze (idempotent append). Metadata first so a batch can be resolved against it.
        new_versions, batches = 0, []
        for path in metadata_paths:
            b = read_metadata_batch(path)
            new_versions += warehouse.ingest_batch(con, b)
            batches.append(b.batch_id)
        for path in article_paths:
            b = read_articles_batch(path)
            n = warehouse.ingest_batch(con, b)
            new_versions += n
            batches.append(b.batch_id)
            log.info("ingested %s: %d rows, %d new record versions", b.source_file, b.row_count, n)

        # 2-3. silver + gold, rebuilt atomically from bronze.
        con.begin()
        try:
            build_silver(con, today=today)
            warehouse.build_gold(con)
            con.commit()
        except Exception:
            con.rollback()
            raise

        # 4. embeddings (incremental via cache) + nearest neighbours.
        newly_embedded = 0
        if embedder is not None:
            newly_embedded = build_article_embeddings(con, embedder, export_dir=output_dir / EMBEDDINGS_DIR.name)
            log.info("embeddings: %d new texts embedded with %s", newly_embedded, embedder.model_name)

        # 5. data contracts: declared grain must hold before anything is published.
        warehouse.assert_grain(con)
        counts = warehouse.row_counts(con)
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success')", [
            run_id, started, datetime.now(timezone.utc).replace(tzinfo=None), PARSER_VERSION,
            embedder.model_name if embedder else None, json.dumps(batches), new_versions,
            json.dumps(counts, sort_keys=True),
        ])
        counts["pipeline_runs"] = counts.get("pipeline_runs", 0) + 1

        # 6. exports
        outputs = warehouse.export_tables(con, output_dir / WAREHOUSE_CSV_DIR.name)
        ai_path = output_dir / "ai_articles_enriched.csv"
        ai_articles_frame(con).to_csv(ai_path, index=False)
        outputs["ai_articles_enriched"] = ai_path
        unmatched = con.execute("""
            SELECT company_name_raw, match_method, company_id, article_count, match_note
            FROM silver_company_resolution WHERE match_method IN ('unmatched', 'ambiguous', 'missing')
            ORDER BY article_count DESC, company_name_raw
        """).df()
        unmatched_path = output_dir / "unmatched_companies.csv"
        unmatched.to_csv(unmatched_path, index=False)
        outputs["unmatched_companies"] = unmatched_path
        if len(unmatched):
            log.warning("%d company names not matched to metadata (see %s)", len(unmatched), unmatched_path)
        return RunResult(run_id, new_versions, newly_embedded, counts, outputs)
    finally:
        if own_con:
            con.close()
