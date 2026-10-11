"""CLI.

    python -m arr_pipeline run                      # ingest default files, rebuild, export
    python -m arr_pipeline run --articles new_batch.csv
    python -m arr_pipeline run --full-refresh       # drop the DB and rebuild from raw files
    python -m arr_pipeline search "LLM funding round" --top-k 5
    python -m arr_pipeline search "LLM funding" --ai-only --min-year 2022 --max-year 2024 --min-arr 50000000
    python -m arr_pipeline company-arr openai
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd

from .config import DEFAULT_ARTICLES_PATH, DEFAULT_DB_PATH, DEFAULT_METADATA_PATH, OUTPUT_DIR


def _cmd_run(args: argparse.Namespace) -> int:
    from .embeddings import make_embedder
    from .pipeline import run_pipeline

    embedder = None if args.embedder == "none" else make_embedder(args.embedder)
    result = run_pipeline(
        article_paths=[Path(p) for p in args.articles],
        metadata_paths=[Path(p) for p in args.metadata],
        db_path=Path(args.db),
        output_dir=Path(args.output_dir),
        embedder=embedder,
        full_refresh=args.full_refresh,
    )
    print(f"run {result.run_id}: {result.new_record_versions} new record versions, "
          f"{result.newly_embedded} texts embedded")
    for table, n in result.row_counts.items():
        print(f"  {table:<28} {n:>6}")
    print(f"outputs written under {args.output_dir}/")
    return 0


def _cmd_search(args: argparse.Namespace) -> int:
    from .embeddings import hybrid_search

    con = duckdb.connect(args.db, read_only=True)
    df = hybrid_search(
        con, args.query, similar_to_article_id=args.like, top_k=args.top_k,
        min_year=args.min_year, max_year=args.max_year, categories=args.category,
        industries=args.industry, ai_related=True if args.ai_only else None, min_arr_usd=args.min_arr,
    )
    with pd.option_context("display.width", 200, "display.max_colwidth", 60):
        print(df.to_string(index=False))
    return 0


def _cmd_company_arr(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db, read_only=True)
    df = con.execute("""
        SELECT as_of_date, arr_usd, revenue_raw, company_name_raw, article_id, source_url, date_is_ambiguous
        FROM fct_arr_observation WHERE company_id = ? ORDER BY as_of_date, article_id
    """, [args.company_id]).df()
    print(df.to_string(index=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="arr_pipeline")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="ingest + rebuild + export")
    run.add_argument("--articles", nargs="+", default=[str(DEFAULT_ARTICLES_PATH)])
    run.add_argument("--metadata", nargs="+", default=[str(DEFAULT_METADATA_PATH)])
    run.add_argument("--output-dir", default=str(OUTPUT_DIR))
    run.add_argument("--embedder", choices=["sentence-transformers", "hashing", "none"],
                     default="sentence-transformers")
    run.add_argument("--full-refresh", action="store_true")
    run.set_defaults(func=_cmd_run)

    search = sub.add_parser("search", help="semantic / hybrid article search")
    search.add_argument("query", nargs="?")
    search.add_argument("--like", help="find articles similar to this article_id instead of a query")
    search.add_argument("--top-k", type=int, default=5)
    search.add_argument("--min-year", type=int)
    search.add_argument("--max-year", type=int)
    search.add_argument("--category", nargs="+")
    search.add_argument("--industry", nargs="+")
    search.add_argument("--ai-only", action="store_true")
    search.add_argument("--min-arr", type=int, help="exclusive lower bound on valid arr_usd")
    search.set_defaults(func=_cmd_search)

    carr = sub.add_parser("company-arr", help="ARR observations over time for one company_id")
    carr.add_argument("company_id")
    carr.set_defaults(func=_cmd_company_arr)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("arr_pipeline").setLevel(logging.DEBUG if args.verbose else logging.INFO)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
