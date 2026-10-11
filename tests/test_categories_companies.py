import pytest

from arr_pipeline.cleaning.categories import normalize_category
from arr_pipeline.cleaning.companies import CompanyResolver, normalize_company_name
from arr_pipeline.transform import company_age, company_size_category

CANONICAL = ["OpenAI", "Amazon Web Services", "Google DeepMind", "Meta AI", "Microsoft",
             "NVIDIA", "SpaceX", "MongoDB", "DataRobot", "Palantir", "Snowflake", "Cloudflare"]


@pytest.mark.parametrize("raw, expected", [
    ("AI/ML", "AI_ML"), ("Artificial Intelligence", "AI_ML"), ("Machine Learning", "AI_ML"),
    ("AI & ML", "AI_ML"), (" ai/ml ", "AI_ML"),
    ("Cloud", "CLOUD"), ("Cloud Services", "CLOUD"), ("Cloud Computing", "CLOUD"),
    ("Big Data", "DATA_ANALYTICS"), ("Analytics", "DATA_ANALYTICS"),
    ("FinTech", "FINTECH"), ("Financial Technology", "FINTECH"), ("Finance", "FINTECH"),
    ("InfoSec", "CYBERSECURITY"), ("Security", "CYBERSECURITY"),
    ("SaaS", "SOFTWARE_SAAS"), ("Enterprise Software", "SOFTWARE_SAAS"),
    ("Quantum Basket Weaving", "OTHER"), ("", None), (None, None),
])
def test_category_taxonomy(raw, expected):
    assert normalize_category(raw) == expected


@pytest.fixture(scope="module")
def resolver():
    return CompanyResolver(CANONICAL)


@pytest.mark.parametrize("raw, company_id, method", [
    ("OpenAI", "openai", "exact"),
    ("Open AI", "openai", "normalized"),
    ("OpenAI Inc.", "openai", "normalized"),
    ("NVIDIA Corporation", "nvidia", "normalized"),
    ("Nvidia", "nvidia", "normalized"),
    ("Mongo DB", "mongodb", "normalized"),
    ("Palantir Technologies", "palantir", "normalized"),
    ("Amazon Web Services (AWS)", "amazon-web-services", "normalized"),
    ("AWS", "amazon-web-services", "alias"),
    ("DeepMind", "google-deepmind", "alias"),
    ("Facebook AI Research", "meta-ai", "alias"),
    ("Azure", "microsoft", "alias"),
    ("The Boring Company / SpaceX", "spacex", "alias"),   # reviewed override
    ("Snowflak", "snowflake", "fuzzy"),                   # typo
])
def test_resolution(resolver, raw, company_id, method):
    m = resolver.resolve(raw)
    assert (m.company_id, m.match_method, m.in_metadata) == (company_id, method, True)


def test_unseen_compound_name_is_not_attributed(resolver):
    m = resolver.resolve("Snowflake / Databricks")
    assert m.match_method == "ambiguous" and m.company_id is None


def test_unknown_company_gets_stable_id_but_is_flagged(resolver):
    m = resolver.resolve("Mistral AI")
    assert (m.company_id, m.match_method, m.in_metadata) == ("mistral-ai", "unmatched", False)


def test_fuzzy_does_not_overmatch(resolver):
    assert resolver.resolve("Cohere").match_method == "unmatched"
    assert resolver.resolve("Cloudera").match_method == "unmatched"


def test_missing_name(resolver):
    assert resolver.resolve("  ").match_method == "missing"


def test_normalize_company_name():
    assert normalize_company_name("The Acme Holdings, Inc.") == "acme"


@pytest.mark.parametrize("n, size", [
    (9_999, "Small"), (10_000, "Medium"), (30_000, "Medium"), (30_001, "Large"), (None, None)])
def test_size_thresholds(n, size):
    assert company_size_category(n) == size


def test_company_age():
    from datetime import date
    assert company_age(2015, date(2023, 1, 1)) == 8
    assert company_age(2024, date(2023, 1, 1)) is None   # founded after article -> data error
    assert company_age(None, date(2023, 1, 1)) is None
