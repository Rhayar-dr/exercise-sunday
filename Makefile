.DEFAULT_GOAL := help
.PHONY: help install run rebuild run-offline batch test test-slow coverage search search-ai similar company-arr latest dq sql clean clean-all

PYTHON ?= .venv/bin/python
DB     ?= output/warehouse.duckdb
TOP_K  ?= 5

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- setup
install: ## Cria o .venv (Python 3.11) e instala as dependências + o pacote
	@if command -v uv >/dev/null 2>&1; then \
		uv venv --python 3.11 .venv && uv pip install --python $(PYTHON) -r requirements.txt -e . ; \
	else \
		python3.11 -m venv .venv && $(PYTHON) -m pip install -r requirements.txt -e . ; \
	fi

# ----------------------------------------------------------------- pipeline
run: ## Roda o pipeline incremental (idempotente) e regenera os CSVs
	$(PYTHON) -m arr_pipeline --db $(DB) run

rebuild: ## Apaga o banco e reconstrói tudo do zero (full refresh)
	$(PYTHON) -m arr_pipeline --db $(DB) run --full-refresh

run-offline: ## Full refresh com embedder 'hashing' (sem baixar modelo; não semântico)
	$(PYTHON) -m arr_pipeline --db $(DB) run --full-refresh --embedder hashing

batch: ## Ingere um novo lote: make batch FILE=data/raw/novo_lote.csv
	@test -n "$(FILE)" || (echo "uso: make batch FILE=caminho/do/lote.csv" && exit 1)
	$(PYTHON) -m arr_pipeline --db $(DB) run --articles $(FILE)

# ----------------------------------------------------------------- tests
test: ## Roda os testes rápidos (offline, ~3s)
	$(PYTHON) -m pytest -q

test-slow: ## Roda também o teste com o modelo sentence-transformers real
	$(PYTHON) -m pytest -q -m "slow or not slow"

coverage: ## Testes com relatório de cobertura
	$(PYTHON) -m pytest -q --cov=arr_pipeline --cov-report=term-missing

# ----------------------------------------------------------------- consultas
search: ## Busca semântica: make search Q="LLM product launch" [TOP_K=5]
	@test -n "$(Q)" || (echo 'uso: make search Q="texto da busca"' && exit 1)
	$(PYTHON) -m arr_pipeline --db $(DB) search "$(Q)" --top-k $(TOP_K)

search-ai: ## Busca híbrida (IA, 2022-2024, ARR > 50M USD): make search-ai Q="regulation"
	@test -n "$(Q)" || (echo 'uso: make search-ai Q="texto da busca"' && exit 1)
	$(PYTHON) -m arr_pipeline --db $(DB) search "$(Q)" --ai-only --min-year 2022 --max-year 2024 --min-arr 50000000 --top-k $(TOP_K)

similar: ## Artigos similares a um artigo: make similar ID=ART0004
	@test -n "$(ID)" || (echo "uso: make similar ID=ART0004" && exit 1)
	$(PYTHON) -m arr_pipeline --db $(DB) search --like $(ID) --top-k $(TOP_K)

company-arr: ## Série de ARR de uma empresa: make company-arr C=openai
	@test -n "$(C)" || (echo "uso: make company-arr C=openai" && exit 1)
	$(PYTHON) -m arr_pipeline --db $(DB) company-arr $(C)

latest: ## ARR mais recente por empresa
	@$(PYTHON) -c "import duckdb; print(duckdb.connect('$(DB)', read_only=True).sql('SELECT company_id, company_name, latest_as_of_date, latest_arr_usd, observation_count FROM company_arr_latest ORDER BY latest_arr_usd DESC').df().to_string(index=False))"

dq: ## Resumo dos problemas de qualidade de dados
	@$(PYTHON) -c "import duckdb; print(duckdb.connect('$(DB)', read_only=True).sql('SELECT issue_type, severity, count(*) AS n FROM dq_issues GROUP BY ALL ORDER BY n DESC').df().to_string(index=False))"

sql: ## Executa SQL arbitrário: make sql QUERY="SELECT * FROM dim_company"
	@test -n "$(QUERY)" || (echo 'uso: make sql QUERY="SELECT ..."' && exit 1)
	@$(PYTHON) -c "import sys, duckdb; print(duckdb.connect('$(DB)', read_only=True).sql(sys.argv[1]).df().to_string(index=False))" "$(QUERY)"

# ----------------------------------------------------------------- limpeza
clean: ## Remove caches (pytest, coverage, __pycache__)
	rm -rf .pytest_cache .coverage htmlcov
	find . -name __pycache__ -type d -not -path './.venv/*' -exec rm -rf {} +

clean-all: clean ## Remove também os outputs gerados (banco + CSVs)
	rm -rf output
