"""Integration tests: run the real pipeline end-to-end on small fixture batches."""
import json
from datetime import date

import pandas as pd
import pytest

from arr_pipeline import warehouse
from arr_pipeline.embeddings import ArticleIndex, HashingEmbedder, hybrid_search, top_k_neighbors
from arr_pipeline.pipeline import run_pipeline

TODAY = date(2026, 1, 1)
HEADER = "article_id,title,company_name,published_date,category,revenue,summary,url,author,word_count\n"
ROWS = [
    "A1,OpenAI raises funding,OpenAI,2023-05-12,AI/ML,$100M,LLM research and model training,http://x/1,Jane,1000",
    "A2,OpenAI reports growth,Open AI,06/30/2023,Artificial Intelligence,€200M,Model revenue grows fast,http://x/2,,900",
    "A3,AWS cloud expansion,AWS,2022-01-10,Cloud,$10M - $20M,New regions for cloud customers,http://x/3,Sam,800",
    "A4,Stripe payments update,Stripe,04-05-2021,FinTech,Not disclosed,Payments platform update,http://x/4,Sam,700",
    "A5,Mystery startup launches,Cohere,2024-02-02,Machine Learning,N/A,Language model startup news,http://x/5,,",
    "A6,Joint venture news,Snowflake / Databricks,2023-03-03,Big Data,$1B,Data platforms partner,http://x/6,,",
    "A7,Bad date article,OpenAI,2023-02-30,AI/ML,$300M,Date is impossible,http://x/7,,",
    "A8,Scale AI big quarter,Scale AI,2023-09-09,Software,£100M,Labeling platform for AI models,http://x/8,,",
]
META = {
    "OpenAI": {"founded_year": 2015, "headquarters": "SF", "employee_count": 1500, "industry": "AI/ML",
               "is_public": False, "stock_ticker": None},
    "Amazon Web Services": {"founded_year": 2006, "headquarters": "Seattle", "employee_count": 40000,
                            "industry": "Cloud Computing", "is_public": True, "stock_ticker": "AMZN"},
    "Stripe": {"founded_year": 2010, "headquarters": "SF", "employee_count": 10000, "industry": "FinTech",
               "is_public": False, "stock_ticker": "STRI"},
    "Snowflake": {"founded_year": 2012, "headquarters": "MT", "employee_count": 7000,
                  "industry": "Cloud Computing", "is_public": True, "stock_ticker": "SNOW"},
    "Scale AI": {"founded_year": 2016, "headquarters": "SF", "employee_count": 900, "industry": "AI/ML",
                 "is_public": False, "stock_ticker": None},
}


@pytest.fixture
def env(tmp_path):
    articles = tmp_path / "batch_001.csv"
    articles.write_text(HEADER + "\n".join(ROWS) + "\n")
    meta = tmp_path / "company_metadata.json"
    meta.write_text(json.dumps(META))
    db = tmp_path / "wh.duckdb"
    out = tmp_path / "out"

    def run(*article_files, **kw):
        return run_pipeline(article_paths=article_files or (articles,), metadata_paths=(meta,), db_path=db,
                            output_dir=out, embedder=HashingEmbedder(), today=TODAY, **kw)

    return {"run": run, "db": db, "out": out, "tmp": tmp_path, "articles": articles}


def _q(env, sql, params=None):
    con = warehouse.connect(env["db"])
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


def test_rerun_is_idempotent(env):
    first = env["run"]()
    second = env["run"]()
    assert second.new_record_versions == 0 and second.newly_embedded == 0
    for table, n in first.row_counts.items():
        if table != "pipeline_runs":
            assert second.row_counts[table] == n, table
    fct1 = pd.read_csv(env["out"] / "warehouse" / "fct_arr_observation.csv")
    assert fct1["observation_id"].is_unique


def test_only_valid_arr_becomes_observations(env):
    env["run"]()
    fct = _q(env, "SELECT * FROM fct_arr_observation ORDER BY article_id")
    # A4 not disclosed, A5 N/A, A6 ambiguous company, A7 impossible date -> excluded
    assert fct["article_id"].tolist() == ["A1", "A2", "A3", "A8"]
    assert fct.set_index("article_id").loc["A2", "arr_usd"] == 220_000_000
    assert fct.set_index("article_id").loc["A3", "arr_usd"] == 15_000_000
    assert set(fct["company_id"]) == {"openai", "amazon-web-services", "scale-ai"}


def test_every_exclusion_is_explained(env):
    env["run"]()
    dq = _q(env, "SELECT entity_id, issue_type FROM dq_issues WHERE entity_type = 'article'")
    issues = set(map(tuple, dq.values))
    assert ("A4", "arr_not_disclosed") in issues
    assert ("A5", "arr_not_available") in issues
    assert ("A5", "company_unmatched") in issues
    assert ("A6", "company_ambiguous") in issues
    assert ("A6", "arr_valid_but_not_modeled") in issues
    assert ("A7", "date_invalid") in issues
    assert ("A4", "date_ambiguous") in issues


def test_lineage_from_observation_to_source_row(env):
    env["run"]()
    row = _q(env, """
        SELECT f.revenue_raw, b.revenue, b.source_file, b.source_row_number, b.published_date
        FROM fct_arr_observation f
        JOIN bronze_articles b ON b.article_id = f.article_id AND b.record_hash = f.source_record_hash
        WHERE f.observation_id = 'ARR-A2'
    """).iloc[0]
    assert row["revenue_raw"] == row["revenue"] == "€200M"
    assert (row["source_file"], row["source_row_number"], row["published_date"]) == ("batch_001.csv", 2, "06/30/2023")


def test_new_batch_updates_and_appends(env):
    env["run"]()
    batch2 = env["tmp"] / "batch_002.csv"
    batch2.write_text(HEADER + "\n".join([
        ROWS[0],  # unchanged re-delivery -> no new version
        "A3,AWS cloud expansion,AWS,2022-01-10,Cloud,$30M,New regions for cloud customers,http://x/3,Sam,800",
        "A9,OpenAI new model,OpenAI,2024-03-01,AI/ML,$1.6B,Frontier model release,http://x/9,,",
    ]) + "\n")
    result = env["run"](batch2)
    assert result.new_record_versions == 2  # A3 v2 + A9
    assert _q(env, "SELECT count(*) n FROM bronze_articles WHERE article_id = 'A3'")["n"][0] == 2
    fct = _q(env, "SELECT * FROM fct_arr_observation").set_index("article_id")
    assert fct.loc["A3", "arr_usd"] == 30_000_000 and fct.loc["A3", "source_batch_id"].startswith("batch_002")
    latest = _q(env, "SELECT * FROM company_arr_latest WHERE company_id = 'openai'").iloc[0]
    assert (latest["latest_arr_usd"], latest["latest_article_id"], latest["observation_count"]) == (1_600_000_000, "A9", 3)


def test_latest_and_quarterly_views(env):
    env["run"]()
    q = _q(env, "SELECT * FROM company_arr_quarterly WHERE company_id = 'openai' ORDER BY year_quarter")
    assert q["year_quarter"].tolist() == ["2023-Q2"]
    assert q.iloc[0]["arr_usd_end_of_quarter"] == 220_000_000 and q.iloc[0]["observation_count"] == 2


def test_enrichment_fields(env):
    env["run"]()
    a = _q(env, "SELECT * FROM article_enriched").set_index("article_id")
    assert a.loc["A2", "company_name"] == "OpenAI" and a.loc["A2", "company_name_raw"] == "Open AI"
    assert a.loc["A2", "company_age"] == 8 and a.loc["A2", "company_size_category"] == "Small"
    assert a.loc["A3", "company_size_category"] == "Large" and a.loc["A3", "industry"] == "CLOUD"
    assert a.loc["A4", "company_size_category"] == "Medium"
    assert a.loc["A8", "is_ai_related"]  # via industry, although category is Software
    assert pd.isna(a.loc["A5", "industry"]) and a.loc["A5", "is_ai_related"]  # via category


def test_ai_export(env):
    env["run"]()
    ai = pd.read_csv(env["out"] / "ai_articles_enriched.csv")
    # A1 (100M, AI cat), A2 (220M), A8 (127M via industry). A3 cloud, A5 N/A, A7 bad date are out.
    assert sorted(ai["article_id"]) == ["A1", "A2", "A8"]
    assert (ai["arr_usd"] > 50_000_000).all()
    required = ["article_id", "title", "company_name", "published_date", "category", "arr_usd", "summary",
                "url", "industry", "founded_year", "headquarters", "employee_count", "is_public",
                "stock_ticker", "company_age", "company_size_category", "embedding"]
    assert all(c in ai.columns for c in required)
    assert len(json.loads(ai["embedding"][0])) == 384
    assert len(json.loads(ai["top_similar_articles"][0])) == 3


def test_every_table_exported(env):
    env["run"]()
    for table in warehouse.EXPORT_TABLES:
        assert (env["out"] / "warehouse" / f"{table}.csv").exists(), table


def test_full_refresh_rebuilds_identically(env):
    env["run"]()
    before = pd.read_csv(env["out"] / "warehouse" / "fct_arr_observation.csv")
    env["run"](full_refresh=True)
    after = pd.read_csv(env["out"] / "warehouse" / "fct_arr_observation.csv")
    pd.testing.assert_frame_equal(before, after)


def test_schema_drift(env):
    extra = env["tmp"] / "batch_extra.csv"
    extra.write_text(HEADER.strip() + ",sentiment\n" + ROWS[0] + ",positive\n")
    env["run"](extra)
    assert json.loads(_q(env, "SELECT extra_columns_json FROM bronze_articles")["extra_columns_json"][0]) == {
        "sentiment": "positive"}
    broken = env["tmp"] / "batch_broken.csv"
    broken.write_text("article_id,title\nZ1,hello\n")
    with pytest.raises(ValueError, match="missing required columns"):
        env["run"](broken)


def test_grain_check_catches_duplicates():
    con = warehouse.connect(None)
    con.execute("CREATE TABLE dim_company AS SELECT 'x' AS company_id UNION ALL SELECT 'x'")
    with pytest.raises(AssertionError, match="dim_company"):
        warehouse.assert_grain(con, ["dim_company"])


# ------------------------------------------------------------------ search
def test_top_similar_excludes_self_and_is_deterministic():
    import numpy as np
    m = np.eye(4, dtype=np.float32)
    m[1] = m[0]  # rows 0 and 1 identical
    idx, scores = top_k_neighbors(m, k=3)
    assert idx[0][0] == 1 and idx[1][0] == 0 and scores[0][0] == 1.0
    assert all(i not in row for i, row in enumerate(idx))


def test_find_similar_and_hybrid(env):
    env["run"]()
    con = warehouse.connect(env["db"])
    try:
        index = ArticleIndex.from_duckdb(con, HashingEmbedder())
        hits = index.find_similar_articles("AWS cloud expansion new regions", top_k=2)
        assert hits[0][0] == "A3" and len(hits) == 2 and hits[0][1] >= hits[1][1]
        assert all(aid != "A1" for aid, _ in index.find_similar_to_article("A1", top_k=5))

        df = hybrid_search(con, "model research", embedder=HashingEmbedder(), ai_related=True,
                           min_year=2022, max_year=2024, min_arr_usd=50_000_000, top_k=10)
        assert set(df["article_id"]) == {"A1", "A2", "A8"}
        assert df["similarity"].is_monotonic_decreasing

        like = hybrid_search(con, similar_to_article_id="A1", top_k=3)
        assert "A1" not in set(like["article_id"]) and len(like) == 3
        stored = con.execute("SELECT top_similar_articles FROM article_embeddings WHERE article_id='A1'").fetchone()[0]
        assert list(stored) == like["article_id"].tolist()
    finally:
        con.close()


@pytest.mark.slow
def test_sentence_transformer_semantics():
    from arr_pipeline.embeddings import SentenceTransformerEmbedder
    e = SentenceTransformerEmbedder()
    v = e.encode(["neural network training", "deep learning models", "quarterly payment volume"])
    assert v.shape == (3, 384)
    assert v[0] @ v[1] > v[0] @ v[2]


def test_cli_run_search_and_company_arr(env, capsys):
    from arr_pipeline.__main__ import main

    db, out = str(env["db"]), str(env["out"])
    meta = str(env["tmp"] / "company_metadata.json")
    assert main(["--db", db, "run", "--articles", str(env["articles"]), "--metadata", meta,
                 "--output-dir", out, "--embedder", "hashing"]) == 0
    assert "fct_arr_observation" in capsys.readouterr().out
    assert main(["--db", db, "search", "cloud regions", "--top-k", "2"]) == 0
    assert "A3" in capsys.readouterr().out
    assert main(["--db", db, "search", "--like", "A1", "--ai-only", "--min-arr", "50000000"]) == 0
    capsys.readouterr()
    assert main(["--db", db, "company-arr", "openai"]) == 0
    printed = capsys.readouterr().out
    assert "A1" in printed and "A2" in printed and "A7" not in printed  # A7 has an invalid date
