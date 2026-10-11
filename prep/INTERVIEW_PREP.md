# Preparação para a Live Review (45 min: review + coding)

> Arquivo pessoal de estudo. **Não vai no ZIP de entrega.**
> As perguntas estão em inglês, como o entrevistador provavelmente vai fazer. As respostas estão em português, com os termos-chave em inglês para você usar na hora.
> Todos os SQL deste arquivo foram executados contra `output/warehouse.duckdb` e funcionam. Rode com `make sql QUERY="..."`.

---

## 0. Pitch de 60 segundos (abra com isso)

> "I built a local medallion-style pipeline on DuckDB.
>
> - **Bronze** stores every source record version as raw strings, keyed by `(article_id, record_hash)`.
> - **Silver** applies pure, unit-tested cleaning functions. Each one returns a status, never a silent null.
> - **Gold** models ARR as an **observation fact**: one row per valid article-reported ARR value, linked back to the exact source row version.
> - Latest and quarterly ARR are **derived views** with explicit tie-break rules, not master data.
>
> Every rebuild is atomic, every table has a declared grain checked before export, and re-runs are idempotent. Embeddings live in DuckDB as `FLOAT[384]`, so hybrid search is a single SQL query."

Números para ter na cabeça:

| Métrica | Valor |
|---|---|
| Artigos | 750 |
| ARR válidos | 558 (400 USD, 55 JPY, 53 EUR, 50 GBP; 58 ranges) |
| Not disclosed / missing / N/A | 85 / 66 / 41 |
| Datas ambíguas | 93 (56 com traço, 37 com barra); 0 inválidas |
| Nomes de empresa brutos → empresas | 46 → 26 (5 fora da metadata) |
| Linhas em `fct_arr_observation` | 558 |
| Linhas em `ai_articles_enriched` | 124 |
| Testes | 125 (~95% de cobertura) |

---

## 1. Data modeling

### Q1. What's the grain of your fact table and why?
Uma linha por **observação de ARR válida reportada**, ou seja, um artigo reportando ARR de uma empresa na `published_date`. A chave é `observation_id = 'ARR-' || article_id`.

Escolhi esse grain porque o dado de origem é uma *afirmação* de um artigo, não um fato contábil da empresa. Se eu modelasse "ARR por empresa por dia", teria que decidir na carga o que fazer com duas notícias no mesmo dia. Mantendo o grain no nível do artigo, nenhuma informação se perde, e a agregação vira uma decisão explícita e documentada nas views derivadas.

### Q2. Why not just put `latest_arr` as a column on the company dimension?
Porque o PDF diz literalmente: *"Do not treat article ARR values as company master data without source lineage and caveats."*

Um `latest_arr` em `dim_company` seria sobrescrito a cada carga e perderia a origem. A `company_arr_latest` é derivada da fact e carrega `latest_observation_id`, `latest_article_id`, `latest_revenue_raw` e `latest_source_url`. Sempre dá para auditar de onde veio o número.

### Q3. Walk me through the tables.
- **Bronze**
  - `ingest_batches`: um arquivo por linha.
  - `bronze_articles` e `bronze_company_metadata`: cada *versão* de cada registro, como string bruta.
- **Silver** (uma linha por entidade, versão atual)
  - `silver_articles`: tipado, com valor bruto e limpo lado a lado.
  - `silver_arr_parse`: resultado do parse de receita de *todos* os artigos, inclusive falhas.
  - `silver_company_resolution`: a decisão de entity resolution para cada nome bruto.
  - `silver_company_metadata`.
- **Gold**
  - `dim_company`, `fct_arr_observation` e `article_enriched` (tabela larga para filtros).
  - `company_arr_latest` e `company_arr_quarterly` (views derivadas materializadas).
  - `dq_issues` (cada exclusão com o motivo) e `article_embeddings`.

### Q4. Why separate `silver_arr_parse` from `silver_articles`?
Para ter um lugar único onde se debuga parse. Ela tem `revenue_raw`, `parse_status`, `error_reason`, `currency_original`, `fx_rate_to_usd` e `parser_version`.

O PDF pede que os dados exportados permitam *"debug failed parses"*. Também facilita uma evolução: se amanhã extrairmos mais de uma métrica por artigo (receita, funding, headcount), vira uma tabela de métricas parseadas sem mexer na tabela de artigos.

### Q5. How do you trace an ARR number back to the source?
```sql
SELECT f.observation_id, f.arr_usd, f.revenue_raw, f.fx_rate_to_usd, f.parser_version,
       b.source_file, b.source_row_number, b.revenue AS as_received
FROM fct_arr_observation f
JOIN bronze_articles b ON b.article_id = f.article_id AND b.record_hash = f.source_record_hash
WHERE f.observation_id = 'ARR-ART0004';
```
O join usa `record_hash`, então aponta para a **versão exata** da linha que gerou o número, mesmo que um lote posterior tenha alterado o artigo.

### Q6. How do you derive latest ARR? What about ties?
Fica em `company_arr_latest`: a observação com maior `as_of_date`. No mesmo dia, o desempate é:
1. data não ambígua primeiro;
2. depois o maior `article_id`.

Ela também traz `previous_arr_usd`, `first_as_of_date` e `observation_count`. Existem 12 observações em 6 pares no mesmo dia, e em todos os casos os valores são iguais.

### Q7. And quarterly ARR? Do you forward-fill?
`company_arr_quarterly` traz o **último valor observado no trimestre** (`arr_usd_end_of_quarter`), mais mínimo, máximo, mediana e contagem. Ela **não faz forward fill**: só existem linhas para trimestres que tiveram observação.

Isso é intencional. Preencher em silêncio esconde que não houve dado novo. Se o analista quiser uma série densa, faz o forward fill explícito na query (está no doc de arquitetura, com `last_value(... IGNORE NULLS)`), e o valor preenchido fica visivelmente separado do observado.

### Q8. Why do companies not in the metadata still get a `company_id`?
Porque o ARR delas é válido e consultável. Cohere, por exemplo, tem 6 observações. Elas recebem um slug estável (`cohere`) e `in_metadata = false`, com os campos de enriquecimento nulos.

Jogar fora seria perder dado. Só não atribuo quando o nome é **ambíguo**, porque aí atribuir à empresa errada é pior do que não atribuir.

### Q9. Why surrogate keys as slugs instead of integers or UUIDs?
São **determinísticas**: o mesmo input gera sempre o mesmo id, e isso é o que garante idempotência e diffs estáveis entre runs. Também são legíveis (`amazon-web-services`).

Trade-off: se a empresa mudar de nome, o slug fica errado. Em produção eu usaria um id mestre vindo de uma tabela de entidades com alias (§6), e o nome viraria atributo.

---

## 2. Cleaning: revenue

### Q10. Walk me through `parse_revenue`.
A gramática é `[moeda] número [escala] [moeda]`, opcionalmente como range `A - B` (também aceita `–`, `—` e `to`).
1. Classifica os não-valores primeiro: `missing`, `not_available`, `not_disclosed`.
2. Quebra o texto em range, se houver.
3. Faz o parse de cada lado. Um lado sem moeda ou escala herda do outro (`$10 - 20M`).
4. Aplica a escala e o câmbio em `Decimal`.
5. Calcula o ponto médio.
6. Arredonda half-up para `int` **uma única vez, no final**.

Nunca levanta exceção: falha vira `status='unparseable'` com `error_reason`.

### Q11. Why `Decimal` instead of float?
JPY ÷ 150 em float gera erro de representação, e arredondar duas vezes (antes e depois do câmbio) pode errar o inteiro final por 1. Com Decimal, a conversão é exata e há um único arredondamento.

O dado tem casos como `£244,094,488 × 1.27 = 309,999,999.76`, que vira **310,000,000**. Esse caso está nos testes.

### Q12. What happens with "Not disclosed"? Why not 0?
Vira `status='not_disclosed'` e `arr_usd = NULL`. **Não entra na fact**, e aparece em `dq_issues` como `arr_not_disclosed`.

Zero seria um valor falso: puxaria médias para baixo e criaria quedas de 100% na série. O PDF diz explicitamente para não tratar esses casos como observações válidas.

### Q13. How do you handle `2,1B`? And `1,23,45`?
- `2,1B` vira 2.1B (vírgula decimal europeia: um ou dois dígitos depois da vírgula).
- `1,234,567` é separador de milhar bem formado.
- `1,23,45` é agrupamento irregular e vira `unparseable`.

Prefiro rejeitar a adivinhar.

### Q14. Range midpoint, is that right?
O PDF pede o ponto médio. Mas guardo `arr_usd_low`, `arr_usd_high` e `is_range = true`, então quem precisar do intervalo tem.

No dado, os ranges são todos ±5% em torno do valor da série (ex.: `$90.2M - $99.8M` = 95M), então o ponto médio bate.

### Q15. What if a range mixes currencies, like `$5M - €6M`?
Vira `unparseable` com o motivo "range mixes currencies". Não existe no dado, mas preferi falhar explicitamente. Se aparecer, o alerta de taxa de unparseable dispara e alguém decide a regra.

---

## 3. Cleaning: dates

### Q16. How do you handle ambiguous dates like `04-05-2021`? ⭐ (pergunta muito provável)
Os formatos são casados explicitamente, sem `dateutil` adivinhando. Para `NN?NN/YYYY` com os dois campos ≤ 12:

- **Barra (`/`) vira US, MM/DD.** Nenhuma data com barra tem o primeiro campo > 12, e 71 têm o segundo > 12. A convenção é claramente americana.
- **Traço (`-`) vira EU, DD-MM.** Das datas com traço não ambíguas, 63 são dia-primeiro e 17 são mês-primeiro. É só a maioria.

Toda linha ambígua recebe `date_is_ambiguous = true` e guarda `date_alternative`. As duas leituras estão sempre no mesmo ano, então filtro por ano é seguro; trimestre e mês podem não ser.

### Q17. How confident are you in that rule? ⭐
Fiz uma validação independente. As séries de ARR são quase monotônicas: 7 quedas em 460 passos não ambíguos. Para cada data ambígua, verifiquei qual leitura cabe na curva de crescimento da empresa.

| Separador | Só a escolhida cabe | Só a alternativa cabe | Ambas cabem | Nenhuma cabe |
|---|---|---|---|---|
| `/` | 16 | 0 | 14 | 0 |
| `-` | 7 | 5 | 28 | 2 |

A regra da barra é forte. A do traço é fraca, e por isso eu sinalizo em vez de confiar cegamente.

**Exemplo concreto (decore):** o ART0226 tem a data `03-11-2022`, que lida como DD-MM vira **3/nov/2022**, com ARR da Anthropic de **$95M**. Mas a Anthropic já estava em $250M em outubro de 2022, e $95M é o patamar de **fev/mar 2022**. A leitura alternativa (11/mar/2022) é a correta. A linha tem a flag, e é a principal "queda" que aparece na detecção de outliers (Q46).

### Q18. Why not use the growth curve to fix the dates automatically?
Porque seria raciocínio circular: usar a métrica para corrigir o timestamp da própria métrica. Se uma empresa realmente teve queda de ARR, eu "corrigiria" a data e esconderia o evento.

Eu usaria esse sinal para **priorizar revisão humana**, ou como desempate só quando uma leitura é consistente e a outra não, gravando `date_resolution_method = 'growth_consistency'` para ficar auditável.

### Q19. ISO timestamps like `2023-05-12T08:00:00Z`?
Uso a parte da data (que é UTC). Se a fonte tivesse fusos diferentes, normalizaria para UTC antes, já que a data do artigo define o trimestre.

### Q20. What's `out_of_range`?
Ano antes de 1990 ou data no futuro. São datas sintaticamente válidas mas implausíveis para uma notícia. Viram status próprio e aparecem em `dq_issues`, em vez de sumir.

---

## 4. Cleaning: categories and companies

### Q21. Why one taxonomy for category and industry?
Para o filtro de IA ser trivial e consistente: `category = 'AI_ML' OR industry = 'AI_ML'`.

A metadata usa "AI/ML" e o artigo usa "Machine Learning", "AI & ML" e "Artificial Intelligence". Com uma taxonomia só, as duas fontes falam a mesma língua. O mapeamento está no código (`CATEGORY_MAPPING`), como o PDF pede. Rótulo desconhecido vira `OTHER`, mantém o valor bruto e gera flag.

### Q22. Explain your company matching cascade.
1. **exact**
2. **alias**: tabela curada, checada primeiro
3. **normalized**: caixa, sufixos legais (Inc., Corporation, Technologies), parênteses e espaços. Cobre "Open AI", "Mongo DB", "NVIDIA Corporation" e "Amazon Web Services (AWS)".
4. **ambiguous**: nomes com múltiplas entidades (`A / B`) não são atribuídos
5. **fuzzy**: difflib ≥ 0.88 **e** pelo menos 0.05 à frente do segundo colocado
6. **unmatched**: recebe slug, com `in_metadata = false`

O método fica gravado em cada linha (`company_match_method`), então dá para filtrar, por exemplo, só matches exatos.

### Q23. Why the margin on fuzzy matching?
Para evitar falso positivo entre nomes parecidos. Um threshold sozinho deixaria "Cohere" virar "Confluent", ou "Cloudera" virar "Cloudflare". A margem exige que o melhor candidato seja claramente melhor que o segundo. Os dois casos estão nos testes.

### Q24. Azure → Microsoft? Isn't Azure a segment? ⭐
No mundo real, sim: ARR do Azure ≠ ARR da Microsoft. Por isso o código tem o conceito `relationship = 'product_of_parent'`, e as views latest e quarterly só usam linhas `self`.

Mas **neste dataset** os valores de "Azure" caem exatamente na série da Microsoft ($79.17B, $83.85B). São variações de nome, não segmento. Quando eu tratei como segmento, o "latest" da Microsoft ficou desatualizado (julho em vez de dezembro de 2024). Então registrei como alias `self` revisado, com a evidência no comentário. Se uma fonte real trouxer receita de segmento, é trocar uma linha da tabela de alias.

### Q25. And "The Boring Company / SpaceX"?
A regra genérica deixa nomes compostos sem atribuição. Mas os 3 valores ($8.5B em dez/2023, $9.945B em nov e dez/2024) estão exatamente na série da SpaceX, então registrei um **override revisado** na tabela de alias.

O ponto é que a regra continua protegendo contra nomes compostos novos e não revisados: "Snowflake / Databricks" fica `ambiguous` (está nos testes). É assim que se opera entity resolution: regras pegam o caso, humanos revisam, e a tabela de alias registra a decisão.

### Q26. The metadata looks wrong (OpenAI = "Data Analytics", Microsoft founded 2018). What did you do?
Usei como veio, porque é a fonte de verdade definida para enriquecimento e "consertar" com conhecimento externo seria inventar dado. Sinalizei só contradições internas: `ticker_on_private_company` (4 casos, ex.: OpenAI com ticker OPEN e `is_public = false`) e `public_company_without_ticker` (5).

Na entrevista, vale mencionar que você *percebeu*. Mostra senso crítico.

### Q27. How is `company_age` calculated?
`published_year − founded_year`, em anos de calendário. Se der negativo (empresa "fundada" depois do artigo), vira NULL e gera flag `company_age_negative`. Não aconteceu neste dado.

Alternativa mais precisa: anos completos usando a data de fundação, que a metadata não tem.

---

## 5. Idempotency, incremental loads, backfills, schema changes ⭐

### Q28. What happens if I run the pipeline twice?
Nada muda:
- `batch_id = <nome-do-arquivo>-<sha256[:12]>`, então o mesmo arquivo gera o mesmo id.
- O bronze usa `INSERT OR IGNORE` em `(article_id, record_hash)`, então um registro repetido é no-op.
- O silver e o gold são reconstruídos com `CREATE OR REPLACE` dentro de **uma transação**, com chaves determinísticas.
- As embeddings vêm de cache por hash de texto, então nada é recalculado.

Há um teste: o segundo run tem 0 versões novas, 0 embeddings novos e as mesmas contagens. O full refresh gera a fact idêntica.

### Q29. How do you guarantee no duplicates?
Três camadas:
1. Chaves determinísticas.
2. PK no bronze.
3. `assert_grain()`: depois de cada run e **antes do export**, cada tabela é checada contra o grain declarado em `TABLE_GRAIN` (zero chaves duplicadas, zero chaves nulas). Se violar, o run falha e nada é publicado.

### Q30. A new batch arrives with an updated version of ART0004. What happens?
A nova versão tem outro `record_hash` e entra como **nova linha** no bronze; a antiga continua lá. A view `bronze_articles_current` escolhe a versão do lote com maior `batch_seq`. O silver e o gold são re-derivados e a observação passa a apontar para o novo `source_record_hash`.

Isso está testado em `test_new_batch_updates_and_appends`.

### Q31. What's a limitation of "latest batch wins"?
Se a fonte reenviar uma versão antiga depois de uma nova (A, depois B, depois A), o PK ignora o segundo A (já existe), e o silver fica com B, que está errado.

Solução: usar um `updated_at` da fonte, ou uma tabela de *sightings* (`article_id`, `record_hash`, `batch_id`) que registra toda vez que uma versão é vista, e escolher pela última aparição.

### Q32. How do you backfill after changing a parsing rule?
1. Muda a regra.
2. Faz bump do `PARSER_VERSION`.
3. Roda o pipeline.

Como o bronze é imutável e guarda o valor bruto, silver e gold são re-derivados com a regra nova para **toda a história**. Cada linha carrega o `parser_version` que a gerou. O `--full-refresh` reconstrói tudo a partir dos arquivos.

Em escala: re-derivar só as partições afetadas (por mês de publicação ou batch), ou fazer shadow run da versão nova, comparar contagens e distribuições com a antiga, e então trocar.

### Q33. A new column appears in the source CSV. What happens?
Vai para `extra_columns_json` no bronze, sem perda nem falha (teste `test_schema_drift`). Promover para coluna tipada é uma decisão explícita: `ALTER TABLE ADD COLUMN` mais backfill.

### Q34. A required column disappears or gets renamed?
- **Some:** a ingestão **falha rápido**, com erro claro, antes de escrever qualquer coisa. Isso é um data contract; null silencioso seria pior.
- **Renomeada:** existe o `COLUMN_RENAMES` em `ingest.py` (ex.: `publish_date` → `published_date`). É só adicionar o mapeamento.

### Q35. Why full rebuild of silver/gold instead of MERGE?
Com 750 linhas, o rebuild leva menos de 1 segundo e é o design mais simples que é obviamente correto, sem risco de estado parcial. Em escala, eu trocaria por `MERGE` pelas chaves afetadas: os `article_id`s dos lotes novos e as empresas cuja metadata ou alias mudou. Depois recalcularia latest e quarterly só para os `company_id`s afetados. As chaves já foram desenhadas para isso.

---

## 6. Semantic search

### Q36. How does `find_similar_articles` work?
1. Embeda a query com `all-MiniLM-L6-v2` (384 dimensões, normalizado L2).
2. Calcula o produto escalar com a matriz de embeddings dos artigos. Como os vetores são normalizados, isso é a similaridade de cosseno.
3. Ordena.

Retorna `[(article_id, score)]`.

### Q37. How does hybrid search work?
Uma query só no DuckDB: filtros SQL em `article_enriched` (ano, categoria, indústria, `is_ai_related`, ARR mínimo, empresa) mais `ORDER BY array_cosine_similarity(embedding, ?::FLOAT[384])`. É parametrizada, sem concatenar strings do usuário (sem SQL injection).

Aceita texto livre ou `similar_to_article_id`, que usa o embedding do próprio artigo e o exclui do resultado.

### Q38. Filter first or rank first?
Aqui é **filter first**: o WHERE reduz o conjunto e depois ranqueio, o que é exato. Com um índice ANN (HNSW), a ordem importa. Pós-filtragem pode retornar menos que k resultados se o filtro for seletivo. Aí as opções são over-fetch (pegar k×10 e filtrar) ou um índice com suporte a filtro.

### Q39. How are `top_similar_articles` computed? Any gotchas?
Calculo a matriz de similaridade N×N, zero a diagonal (para excluir o próprio artigo) e faço argsort.

O gotcha: os resumos vêm de uns 12 templates, então há muitos textos **idênticos** com score empatado em 1.0. Arredondo para 6 casas decimais (senão ruído de float muda a ordem entre runs) e uso sort estável por `article_id`. O resultado é determinístico.

### Q40. Is the search quality good?
É razoável para o dado disponível. Mas título e resumo são templates sintéticos, então muitos vizinhos são "mesmo template, empresa diferente". Com o corpo real dos artigos, faria chunking e embedding dos chunks.

Para medir qualidade, montaria um pequeno conjunto rotulado de pares query → relevante e calcularia recall@k e MRR, comparando modelos.

### Q41. How do embeddings stay incremental?
`embedding_cache` tem chave `(text_hash, model_name)`. Só texto novo é embedado: no segundo run, 0 textos. Textos idênticos compartilham o embedding (750 artigos viraram 673 textos únicos). Trocar de modelo não apaga o cache antigo; os dois convivem até a troca.

### Q42. How would this scale to 10M articles?
- **Busca:** a força bruta em numpy fica cara; uso ANN (DuckDB `vss` com HNSW, pgvector, ou um banco vetorial gerenciado).
- **Top-k de todos os artigos:** N×N não cabe na memória; faço em blocos ou via ANN.
- **Embedding:** vira um job separado em lote (GPU), com chave pelo hash do conteúdo.
- **Índice:** reindexação incremental quando aparecem `text_hash`es novos.

---

## 7. Production / scale ("how would this run beyond a local batch?") ⭐

### Q43. How would you productionize this?
| Aspecto | Proposta |
|---|---|
| Landing | Arquivos imutáveis em S3/GCS particionados por data de ingestão, com notificação por evento |
| Orquestração | Airflow ou Dagster: ingest → silver → gold → embed → checks → publish. Cada task idempotente, então retry é seguro |
| Storage | Iceberg ou Delta (ou Snowflake/Databricks), com `MERGE` pelas mesmas chaves e time travel para auditoria |
| Data quality | Os grain checks atuais viram testes que bloqueiam o publish (dbt tests ou Great Expectations), com thresholds como "unparseable < 2%" e "unmatched < 5%". Se violar, o gold não é publicado e os consumidores continuam no último snapshot bom |
| Observabilidade | Métricas por run (já tenho `pipeline_runs` e `dq_issues`), SLAs de freshness e alertas |

### Q44. FX rates are static. What would you change?
Criaria uma tabela `dim_fx_rate(currency, date, rate, source)` com join pela `as_of_date`. Manteria o `fx_rate_set` em cada linha para reprodutibilidade, e uma mudança de câmbio seria um backfill controlado. Detalhe importante: ARR em moeda estrangeira convertido por spot muda com o câmbio, então também guardaria o valor original (já guardo `amount_original` e `currency_original`).

### Q45. How do you handle late-arriving or corrected articles?
O versionamento já cobre correção (nova versão no bronze). Para responder "o que sabíamos na data X?", adicionaria colunas bitemporais: `valid_from` (data do artigo) e `recorded_at` (quando ingerimos). Para retratações, tombstones.

---

## 8. SQL ao vivo (todas testadas)

### Q46. "Find suspicious ARR observations" (detecção de anomalia)
```sql
SELECT * FROM (
  SELECT company_id, as_of_date, arr_usd, date_is_ambiguous,
         lag(arr_usd) OVER w AS prev_arr,
         arr_usd / lag(arr_usd) OVER w AS ratio
  FROM fct_arr_observation WHERE company_relationship = 'self'
  WINDOW w AS (PARTITION BY company_id ORDER BY as_of_date, article_id))
WHERE ratio < 0.9 OR ratio > 2 ORDER BY ratio;
```
O primeiro resultado é a Anthropic em 2022-11-03 (ratio 0.38), que é exatamente o caso de data ambígua da Q17.

### Q47. "Year-over-year ARR growth per company"
```sql
WITH y AS (
  SELECT company_id, as_of_year AS year,
         arg_max(arr_usd, (as_of_date, article_id)) AS arr_end_of_year
  FROM fct_arr_observation WHERE company_relationship = 'self'
  GROUP BY ALL)
SELECT company_id, year, arr_end_of_year,
       lag(arr_end_of_year) OVER (PARTITION BY company_id ORDER BY year) AS prev_year,
       round(arr_end_of_year / lag(arr_end_of_year) OVER (PARTITION BY company_id ORDER BY year) - 1, 3) AS yoy_growth
FROM y ORDER BY company_id, year;
```
`arg_max(valor, (data, id))` devolve o valor da linha com a maior data, com desempate por id. É a forma idiomática de pegar o "último valor" no DuckDB. Em outros bancos, use `row_number() ... = 1`.

### Q48. "Which companies grew the most from 2022 to 2024?"
```sql
WITH y AS (SELECT company_id, as_of_year AS year, arg_max(arr_usd, (as_of_date, article_id)) AS arr
           FROM fct_arr_observation WHERE company_relationship = 'self' GROUP BY ALL)
SELECT a.company_id, a.arr AS arr_2022, b.arr AS arr_2024, round(b.arr / a.arr, 2) AS multiple
FROM y a JOIN y b ON a.company_id = b.company_id AND a.year = 2022 AND b.year = 2024
ORDER BY multiple DESC LIMIT 5;
```
Resultado: Anthropic 4.14×, NVIDIA 3.49×, Scale AI 2.29×, Cohere 2.22×, Databricks 2.05×.

### Q49. "What was each company's ARR as of 2023-06-30?" (point-in-time)
```sql
SELECT company_id, arg_max(arr_usd, (as_of_date, article_id)) AS arr_usd, max(as_of_date) AS observed_on
FROM fct_arr_observation
WHERE as_of_date <= DATE '2023-06-30' AND company_relationship = 'self'
GROUP BY company_id ORDER BY arr_usd DESC;
```
Mostre também `observed_on`. Um valor "as of junho" observado em janeiro está velho, e o analista precisa saber disso.

### Q50. "Show the parse-status breakdown per batch" (monitoramento)
```sql
SELECT a.source_batch_id, p.parse_status, count(*) AS n,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY a.source_batch_id), 1) AS pct
FROM silver_articles a JOIN silver_arr_parse p USING (article_id)
GROUP BY a.source_batch_id, p.parse_status ORDER BY n DESC;
```
Atenção: com window sobre agregado, use `GROUP BY` explícito. O `GROUP BY ALL` dá erro de binder nesse caso; aconteceu comigo testando.

### Q51. "Same-day duplicate observations — do they conflict?"
```sql
SELECT company_id, as_of_date, count(*) AS n, count(DISTINCT arr_usd) AS distinct_values
FROM fct_arr_observation GROUP BY ALL HAVING count(*) > 1;
```
São 6 pares, todos com o mesmo valor, então não há conflito.

---

## 9. Live coding: features prováveis (com o caminho da solução)

Para cada uma: **onde mexer → o que mudar → qual teste adicionar.** Treine fazer 2 ou 3 delas de verdade antes da entrevista.

### C1. "Add support for CAD (Canadian dollar, ×0.74)"
1. `config.py`: `FX_TO_USD["CAD"] = Decimal("0.74")`.
2. `cleaning/revenue.py`: adicionar `"C$": "CAD"` em `_CURRENCY_SYMBOLS`, incluir `C\$|CAD` no grupo `pre` e `CAD` no grupo `post` de `_AMOUNT_RE`. O lookup usa `c.upper()`, então a chave `"C$"` funciona.
3. Teste: `assert parse_revenue_usd("C$100M") == 74_000_000`.
4. Bump do `PARSER_VERSION` e rerun (= backfill).

### C2. "Add YoY growth to the quarterly table"
No `GOLD_SQL` de `company_arr_quarterly`, adicionar:
```sql
lag(arr_usd_end_of_quarter, 4) OVER (PARTITION BY company_id ORDER BY year, quarter)
```
**Cuidado:** só existem trimestres observados, então `lag(…, 4)` não é necessariamente o mesmo trimestre do ano anterior. Faça um self-join por `(company_id, year - 1, quarter)`:
```sql
LEFT JOIN q AS prev ON prev.company_id = q.company_id AND prev.year = q.year - 1 AND prev.quarter = q.quarter
```
Esse é o tipo de pegadinha que o entrevistador quer ver você notar.

### C3. "Add an outlier flag to the fact table"
No SQL da fact, adicione `arr_usd / lag(arr_usd) OVER (PARTITION BY company_id ORDER BY as_of_date, article_id) AS ratio_vs_prev` e `ratio_vs_prev < 0.5 OR ratio_vs_prev > 3 AS is_outlier`. Adicione um `UNION ALL` no `dq_issues` com `issue_type = 'arr_outlier'`. **Sinalize, não remova.**

### C4. "Support a new date format, e.g. `2023.05.12` or `12.05.2023`"
- `dates.py`: o `_NUMERIC_RE` já aceita `.` como separador (`[/.-]`), e como só `/` é tratado como US, `12.05.2023` já cai na regra EU (DD.MM), que é a convenção usual para ponto. Falta só um teste que documente isso.
- Para `YYYY.MM.DD`, estenda `_ISO_SLASH_RE` para `[/.]`.
- Teste os dois casos e o ambíguo.

### C5. "Add an `author` filter to hybrid search"
Em `embeddings.hybrid_search`, adicionar o parâmetro `authors: Optional[Sequence[str]] = None`. Depois:
```python
where.append("list_contains(?, a.author)"); params.append(list(authors))
```
`author` já existe em `article_enriched`. Expor no CLI com `--author` e testar.

### C6. "Add a new company alias"
Uma linha em `COMPANY_ALIASES` (`"fb ai": ("Meta AI", SELF)`), mais um caso no parametrize de `test_resolution`, mais um rerun. A chave é normalizada por `normalize_company_name`, então não precisa se preocupar com caixa ou pontuação.

### C7. "Only process new articles (true incremental)"
Hoje o silver é reconstruído inteiro. Para incremental:
1. Pegar os `article_id`s com `record_hash` novo no batch (já calculo `new_versions` em `ingest_batch`).
2. Rodar `clean_articles` só neles.
3. `DELETE FROM silver_articles WHERE article_id IN (...)` e `INSERT` (ou `INSERT OR REPLACE` com PK).
4. Recalcular `company_arr_latest` e `quarterly` só para os `company_id`s afetados.

Ressalva: se a tabela de alias ou o parser mudarem, isso exige full rebuild, então o pipeline deve detectar mudança de `PARSER_VERSION` e forçar.

### C8. "Expose a `get_company_arr_history(company_id)` Python function"
```python
def get_company_arr_history(con, company_id: str) -> pd.DataFrame:
    return con.execute("""
        SELECT as_of_date, arr_usd, revenue_raw, article_id, source_url, date_is_ambiguous
        FROM fct_arr_observation WHERE company_id = ? ORDER BY as_of_date, article_id
    """, [company_id]).df()
```
Use parâmetro `?` e nunca f-string com input do usuário.

### C9. "Dense quarterly series with forward fill" → está no doc de arquitetura (§7). Use `last_value(... IGNORE NULLS)` sobre um calendário gerado com `range()`, e mantenha uma coluna `observed` separada do valor preenchido.

### C10. "Add a data-quality threshold that fails the pipeline"
Em `pipeline.run_pipeline`, depois do `build_gold`:
```python
rate = con.execute("SELECT avg((parse_status = 'unparseable')::INT) FROM silver_arr_parse").fetchone()[0]
if rate > MAX_UNPARSEABLE_RATE:
    raise RuntimeError(f"unparseable rate {rate:.1%} above threshold")
```
Isso acontece **antes** do export, então nada é publicado. Coloque a constante em `config.py` e teste com uma fixture cheia de lixo.

---

## 10. Trade-offs e perguntas "por quê"

### Q52. Why DuckDB and CSVs instead of Postgres?
O DuckDB é embarcado (sem servidor), colunar e analítico, tem tipo `FLOAT[N]` com similaridade de cosseno nativa e foi sugerido no PDF. Os CSVs por tabela são **exigência do PDF**: são exports do DuckDB, não o storage. O Postgres seria OLTP, exigiria servidor e precisaria da extensão pgvector.

### Q53. Why pandas for silver and SQL for gold?
O silver aplica lógica linha a linha complexa (parsers com regex, Decimal, entity resolution), que é mais testável como funções Python puras. O gold é álgebra relacional (joins, window functions, agregação), e SQL é mais claro e declarativo para isso, além de documentar o modelo. Em escala, as funções do silver virariam UDFs, ou o silver iria para Spark.

### Q54. Why not `dateutil.parser.parse`?
Ele adivinha em silêncio: `04-05-2021` vira 5 de abril sem avisar. Eu precisava saber *qual* regra gerou cada data e marcar ambiguidade, então casar formatos explicitamente era requisito, não preferência.

### Q55. What would you do with more time?
1. Tabela de alias como **dado**, com dono, data e evidência, e uma fila de revisão para `unmatched`, `ambiguous` e `fuzzy`.
2. Uma sightings table para corrigir a limitação do "latest batch wins".
3. Câmbio histórico.
4. Testes de qualidade com thresholds bloqueando o publish.
5. Avaliação formal da busca (recall@k).
6. Resolução de datas assistida por consistência de crescimento, com revisão humana.

### Q56. What was the hardest/most interesting part?
Sugestão: as **datas ambíguas**. Conte a história: regra baseada nas linhas não ambíguas, depois a validação independente pela curva de crescimento, depois o achado de que a regra do traço é fraca, e o exemplo do ART0226. Mostra rigor e honestidade sobre a incerteza.

---

## 11. Dicas para a sessão

- **Antes:** `make rebuild && make test` para garantir que tudo roda. Tenha o terminal aberto com `make help`.
- **Arquivos para ter abertos:** `cleaning/revenue.py`, `cleaning/dates.py`, `warehouse.py` (`GOLD_SQL`) e `pipeline.py`.
- **No live coding:** fale o que vai fazer antes de digitar. Escreva o teste junto. Rode `make test` no fim.
- **Se não souber algo:** diga o trade-off que você considera e como validaria. É melhor que inventar.
- **Seja transparente sobre a IA.** Você declarou o uso, e eles recomendaram. O que eles avaliam é se você entende e consegue estender o código.
