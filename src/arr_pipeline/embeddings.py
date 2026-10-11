"""Article embeddings, cosine similarity search, and SQL+vector hybrid search.

* Text embedded = ``title + ". " + summary`` (per spec).
* Embeddings are L2-normalized, so cosine similarity == dot product.
* Embeddings are cached in DuckDB by (sha256(text), model_name): re-runs and
  new batches only embed text that has not been seen before.
* Stored as DuckDB ``FLOAT[384]`` (fixed-size ARRAY) so hybrid search can call
  ``array_cosine_similarity`` next to ordinary SQL filters.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol, Sequence

import duckdb
import numpy as np
import pandas as pd

from .config import EMBEDDING_DIM, EMBEDDING_MODEL_NAME, TOP_SIMILAR_K


class Embedder(Protocol):
    model_name: str
    dim: int

    def encode(self, texts: Sequence[str]) -> np.ndarray: ...


class SentenceTransformerEmbedder:
    """all-MiniLM-L6-v2 via sentence-transformers (downloaded once, then cached locally)."""

    def __init__(self, model_name: str = EMBEDDING_MODEL_NAME):
        from sentence_transformers import SentenceTransformer  # heavy import, keep lazy

        self.model_name = model_name
        self._model = SentenceTransformer(model_name, device="cpu")
        get_dim = getattr(self._model, "get_embedding_dimension", None) or self._model.get_sentence_embedding_dimension
        self.dim = get_dim()

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        return self._model.encode(list(texts), batch_size=64, normalize_embeddings=True,
                                  convert_to_numpy=True, show_progress_bar=False).astype(np.float32)


class HashingEmbedder:
    """Deterministic, dependency-free bag-of-words embedder.

    Used in tests and as an offline fallback (``--embedder hashing``). It is
    lexical, not semantic - fine for exercising the plumbing, not for quality.
    """

    def __init__(self, dim: int = EMBEDDING_DIM):
        self.model_name = f"hashing-bow-{dim}"
        self.dim = dim

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, text in enumerate(texts):
            for tok in "".join(c.lower() if c.isalnum() else " " for c in text).split():
                h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
                out[i, h % self.dim] += 1.0 if (h >> 64) & 1 else -1.0
        return _normalize(out)


def make_embedder(kind: str = "sentence-transformers") -> Embedder:
    if kind == "hashing":
        return HashingEmbedder()
    return SentenceTransformerEmbedder()


def _normalize(m: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(m, axis=1, keepdims=True)
    return (m / np.where(norms == 0, 1, norms)).astype(np.float32)


def article_text(title: Optional[str], summary: Optional[str]) -> str:
    return ". ".join(s.strip().rstrip(".") for s in (title, summary) if s and str(s).strip()) + "."


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def top_k_neighbors(matrix: np.ndarray, k: int = TOP_SIMILAR_K) -> tuple[np.ndarray, np.ndarray]:
    """For each row, indices/scores of the k most similar *other* rows.

    Scores are rounded to 6 dp before ranking so that float noise between
    identical texts cannot reorder ties; ties then resolve by row order
    (rows are sorted by article_id), making output deterministic.
    """
    sims = np.round(matrix @ matrix.T, 6)
    np.fill_diagonal(sims, -np.inf)
    k = min(k, max(len(matrix) - 1, 0))
    idx = np.argsort(-sims, axis=1, kind="stable")[:, :k]
    return idx, np.take_along_axis(sims, idx, axis=1)


# --------------------------------------------------------------------------- build
def build_article_embeddings(con: duckdb.DuckDBPyConnection, embedder: Embedder,
                             export_dir: Optional[Path] = None) -> int:
    """(Re)build gold ``article_embeddings`` from silver_articles + the embedding cache.

    Returns the number of texts newly embedded (0 on an unchanged re-run).
    """
    articles = con.execute(
        "SELECT article_id, title, summary FROM silver_articles ORDER BY article_id").df()
    articles["text"] = [article_text(t, s) for t, s in zip(articles["title"], articles["summary"])]
    articles["text_hash"] = articles["text"].map(text_hash)

    cached = {r[0] for r in con.execute(
        "SELECT text_hash FROM embedding_cache WHERE model_name = ?", [embedder.model_name]).fetchall()}
    todo = articles.loc[~articles["text_hash"].isin(cached)].drop_duplicates("text_hash")
    if len(todo):
        vectors = embedder.encode(todo["text"].tolist())
        if vectors.shape[1] != EMBEDDING_DIM:
            raise ValueError(f"embedder dim {vectors.shape[1]} != schema dim {EMBEDDING_DIM}")
        new = pd.DataFrame({"text_hash": todo["text_hash"].values, "model_name": embedder.model_name,
                            "embedding": list(vectors)})
        con.register("_new", new)
        con.execute(f"INSERT OR IGNORE INTO embedding_cache "
                    f"SELECT text_hash, model_name, embedding::FLOAT[{EMBEDDING_DIM}] FROM _new")
        con.unregister("_new")

    emb = con.execute("""
        SELECT text_hash, embedding FROM embedding_cache WHERE model_name = ?
    """, [embedder.model_name]).df().set_index("text_hash")["embedding"]
    matrix = np.vstack([np.asarray(emb[h], dtype=np.float32) for h in articles["text_hash"]]) \
        if len(articles) else np.zeros((0, EMBEDDING_DIM), dtype=np.float32)

    ids = articles["article_id"].to_numpy()
    idx, scores = top_k_neighbors(matrix)
    gold = pd.DataFrame({
        "article_id": ids,
        "model_name": embedder.model_name,
        "embedded_text": articles["text"].values,
        "text_hash": articles["text_hash"].values,
        "embedding": list(matrix),
        "top_similar_articles": [list(ids[row]) for row in idx],
        "top_similar_scores": [[float(s) for s in row] for row in scores],
    })
    con.register("_gold", gold)
    con.execute(f"""
        CREATE OR REPLACE TABLE article_embeddings AS
        SELECT article_id, model_name, embedded_text, text_hash,
               embedding::FLOAT[{EMBEDDING_DIM}] AS embedding,
               top_similar_articles::VARCHAR[] AS top_similar_articles,
               top_similar_scores::DOUBLE[] AS top_similar_scores
        FROM _gold
    """)
    con.unregister("_gold")

    if export_dir is not None:  # reusable numpy + id sidecar outside the DB
        export_dir.mkdir(parents=True, exist_ok=True)
        np.save(export_dir / "article_embeddings.npy", matrix)
        (export_dir / "article_ids.json").write_text(json.dumps(
            {"model_name": embedder.model_name, "article_ids": ids.tolist()}, indent=0))
    return len(todo)


# --------------------------------------------------------------------------- search
@dataclass
class ArticleIndex:
    """In-memory cosine index over article embeddings (fine to ~1M rows with numpy)."""

    article_ids: np.ndarray
    matrix: np.ndarray
    embedder: Optional[Embedder] = None

    @classmethod
    def from_duckdb(cls, con: duckdb.DuckDBPyConnection, embedder: Optional[Embedder] = None) -> "ArticleIndex":
        df = con.execute("SELECT article_id, embedding FROM article_embeddings ORDER BY article_id").df()
        matrix = np.vstack(df["embedding"].map(lambda v: np.asarray(v, dtype=np.float32)).to_list())
        return cls(df["article_id"].to_numpy(), matrix, embedder)

    def _query_vector(self, query_text: str) -> np.ndarray:
        if self.embedder is None:
            raise ValueError("ArticleIndex needs an embedder to search by free text")
        return _normalize(self.embedder.encode([query_text]))[0]

    def search_vector(self, vector: np.ndarray, top_k: int = 5,
                      exclude: Sequence[str] = ()) -> list[tuple[str, float]]:
        scores = np.round(self.matrix @ vector, 6)
        order = np.argsort(-scores, kind="stable")
        excluded = set(exclude)
        out = [(str(self.article_ids[i]), float(scores[i])) for i in order if self.article_ids[i] not in excluded]
        return out[:top_k]

    def find_similar_articles(self, query_text: str, top_k: int = 5) -> list[tuple[str, float]]:
        """Top-k (article_id, cosine_similarity) for a free-text query."""
        return self.search_vector(self._query_vector(query_text), top_k)

    def find_similar_to_article(self, article_id: str, top_k: int = 5) -> list[tuple[str, float]]:
        pos = np.flatnonzero(self.article_ids == article_id)
        if not len(pos):
            raise KeyError(article_id)
        return self.search_vector(self.matrix[pos[0]], top_k, exclude=[article_id])


def find_similar_articles(query_text: str, top_k: int = 5, con: Optional[duckdb.DuckDBPyConnection] = None,
                          embedder: Optional[Embedder] = None) -> list[tuple[str, float]]:
    """Spec-shaped convenience entry point: returns [(article_id, score), ...]."""
    from .config import DEFAULT_DB_PATH

    own = con is None
    con = con or duckdb.connect(str(DEFAULT_DB_PATH), read_only=True)
    try:
        embedder = embedder or _embedder_for(con)
        return ArticleIndex.from_duckdb(con, embedder).find_similar_articles(query_text, top_k)
    finally:
        if own:
            con.close()


def _embedder_for(con: duckdb.DuckDBPyConnection) -> Embedder:
    """Pick the embedder matching the model the stored vectors were built with."""
    model = con.execute("SELECT any_value(model_name) FROM article_embeddings").fetchone()[0]
    return HashingEmbedder() if model and model.startswith("hashing") else SentenceTransformerEmbedder()


def hybrid_search(
    con: duckdb.DuckDBPyConnection,
    query_text: Optional[str] = None,
    *,
    similar_to_article_id: Optional[str] = None,
    embedder: Optional[Embedder] = None,
    top_k: int = 10,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    categories: Optional[Sequence[str]] = None,
    industries: Optional[Sequence[str]] = None,
    ai_related: Optional[bool] = None,
    min_arr_usd: Optional[int] = None,
    company_ids: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """SQL filters on article_enriched + cosine similarity ranking, in one DuckDB query.

    Exactly one of ``query_text`` / ``similar_to_article_id`` is required.
    ``min_arr_usd`` is an exclusive bound and only considers *valid* ARR values.
    """
    if (query_text is None) == (similar_to_article_id is None):
        raise ValueError("provide exactly one of query_text or similar_to_article_id")

    where, params = ["TRUE"], []
    if similar_to_article_id is not None:
        row = con.execute("SELECT embedding FROM article_embeddings WHERE article_id = ?",
                          [similar_to_article_id]).fetchone()
        if row is None:
            raise KeyError(similar_to_article_id)
        qvec = np.asarray(row[0], dtype=np.float32)
        where.append("e.article_id <> ?")
        params.append(similar_to_article_id)
    else:
        embedder = embedder or _embedder_for(con)
        qvec = _normalize(embedder.encode([query_text]))[0]

    if min_year is not None:
        where.append("a.published_year >= ?"); params.append(min_year)
    if max_year is not None:
        where.append("a.published_year <= ?"); params.append(max_year)
    if categories:
        where.append("list_contains(?, a.category)"); params.append(list(categories))
    if industries:
        where.append("list_contains(?, a.industry)"); params.append(list(industries))
    if ai_related is not None:
        where.append("a.is_ai_related = ?"); params.append(ai_related)
    if min_arr_usd is not None:
        where.append("a.arr_status = 'valid' AND a.arr_usd > ?"); params.append(min_arr_usd)
    if company_ids:
        where.append("list_contains(?, a.company_id)"); params.append(list(company_ids))

    sql = f"""
        SELECT a.article_id, a.title, a.company_name, a.published_date, a.category, a.industry,
               a.arr_usd, a.url,
               round(array_cosine_similarity(e.embedding, ?::FLOAT[{EMBEDDING_DIM}]), 6) AS similarity
        FROM article_embeddings e JOIN article_enriched a USING (article_id)
        WHERE {' AND '.join(where)}
        ORDER BY similarity DESC, a.article_id
        LIMIT ?
    """
    return con.execute(sql, [qvec.tolist(), *params, top_k]).df()
