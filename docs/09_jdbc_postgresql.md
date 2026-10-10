# Aula 09 — JDBC, PostgreSQL e paralelismo na ingestão

> **Fontes:** [`aula_09_jdbc.ipynb`](../notebooks/aula_09_jdbc.ipynb), [`desafio_09_jdbc.ipynb`](../desafios/desafio_09_jdbc.ipynb) e arquivo SQL enviado em `notebooks/CREATE TABLE public.sql`.

## 1. Integração entre containers

Nesta aula a origem deixou de ser apenas arquivo. **PostgreSQL** foi executado em um container Docker separado do Spark; a conexão ocorre pela rede interna do Compose, com hostname `postgres` e banco `spark_db`:

```text
container Spark/Jupyter
         ↓ JDBC
jdbc:postgresql://postgres:5432/spark_db
         ↓
container PostgreSQL
```

Dentro do container Spark, `localhost` aponta para **o próprio container Spark**, não para o PostgreSQL. O laboratório usou o driver PostgreSQL em `/opt/jdbc/postgresql.jar` e incluiu no `SparkSession`:

```python
spark = (
    SparkSession.builder
    .appName("aula_09_jdbc")
    .config("spark.jars", "/opt/jdbc/postgresql.jar")
    .getOrCreate()
)
```

A configuração de usuário/senha no notebook utiliza `os.getenv("POSTGRES_PASSWORD")` após `load_dotenv()`. **Não coloque segredos no Git ou na documentação**; arquivos `.env` devem permanecer ignorados pelo Git.

## 2. JDBC como ponte de tipos e operações

Na leitura, `spark.read.jdbc(url, table, properties)` devolve um DataFrame; `table` pode ser uma tabela física existente ou uma **subquery com alias** (por exemplo `(SELECT 1 AS conexao_ok) AS teste`). Isso não cria uma tabela persistente no PostgreSQL.

Na escrita, `df.write.jdbc(...)` envia as linhas para uma tabela de destino; uma action de escrita efetivamente executa a cadeia necessária. Foram utilizados `append` e `overwrite`:

```text
append    → adiciona linhas; NÃO realiza deduplicação/upsert automaticamente
overwrite → substitui o conteúdo; dependendo das opções, pode recriar a tabela
```

No início do notebook, o CSV de pedidos foi convertido em Parquet, relido e enviado a `public.staging_pedidos`; depois essa tabela foi lida de volta. A inspeção de schema mostrou mapeamentos como `integer ↔ IntegerType`, `date ↔ DateType`, `text ↔ StringType` e `numeric ↔ DecimalType`. O nome dos tipos varia porque Spark e PostgreSQL têm sistemas de tipos diferentes.

## 3. Leitura JDBC simples: onde aparece o gargalo?

Foi criada uma tabela de teste com cerca de **2.000.000 registros** (`public.fato_pedidos_large`) para testar leitura por JDBC. A leitura sem opções de particionamento retornou **uma Spark Partition**:

```python
df_fato_large = spark.read.jdbc(
    url=jdbc_url,
    table="public.fato_pedidos_large",
    properties=credencias,
)
df_fato_large.rdd.getNumPartitions()  # 1 no experimento
```

Isso pode limitar o paralelismo de entrada: um cluster com vários executores não cria automaticamente várias partições JDBC apenas porque o dataset é grande.

## 4. Leitura paralela: quatro parâmetros

```python
df_fato_large2 = spark.read.jdbc(
    url=jdbc_url,
    table="public.fato_pedidos_large",
    column="pedido_id",
    lowerBound=1,
    upperBound=2_000_000,
    numPartitions=4,
    properties=credencias,
)
```

- `column` (equivalente à opção `partitionColumn`) indica **qual coluna** será usada para montar faixas de leitura. Tipicamente aceita tipos numéricos, data ou timestamp.
- `lowerBound` é a **referência inferior** para calcular os intervalos.
- `upperBound` é a **referência superior** para calcular os intervalos.
- `numPartitions` determina a quantidade desejada de divisões; também pode representar o **limite de conexões simultâneas de leitura** que essas divisões provocam.

Em linhas gerais, o intervalo entre os bounds é dividido em faixas próximas de `(upperBound - lowerBound) / numPartitions`. **Bounds não são filtro rígido**: a primeira e a última faixa podem receber registros além dos limites informados.

### Diferença decisiva: valores x quantidade de linhas

```text
COUNT(*)         → quantas linhas existem na tabela/consulta?
MIN(pedido_id)   → menor VALOR da coluna de particionamento
MAX(pedido_id)   → maior VALOR da coluna de particionamento
```

Os bounds precisam corresponder à **faixa de valores da coluna**, não à quantidade de linhas. **Mesmo bounds corretos não garantem distribuição uniforme** caso existam buracos ou concentrações de registros nos intervalos.

## 5. Experimento real de distribuição

Com `pedido_id` sequencial de 1 até 2 milhões e `numPartitions=4`, o notebook e o desafio registraram distribuição praticamente perfeita:

| Spark Partition | Registros |
|---|---:|
| P0 | 500.001 |
| P1 | 499.999 |
| P2 | 499.999 |
| P3 | 500.001 |

A contagem foi feita **antes da agregação por partição**, usando:

```python
(
    df_fato_large2
    .withColumn("spark_partition", F.spark_partition_id())
    .groupBy("spark_partition").count()
    .orderBy("spark_partition")
    .show()
)
```

Esse equilíbrio foi consequência da distribuição **realmente uniforme** dos IDs do experimento. Um ID numérico ou uma chave primária **não assegura** isso em outros bancos.

## 6. Bounds inconsistentes não descartam registros

No desafio, o limite superior foi trocado de `2_000_000` para `500_000`, com a tabela completa de 2 milhões:

| Spark Partition | Registros observados |
|---|---:|
| P0 | 125.000 |
| P1 | 124.999 |
| P2 | 124.999 |
| P3 | 1.625.002 |

O total continuou **2 milhões**. A última faixa não parou em 500 mil; ela recebeu os registros restantes. O resultado foi um **desequilíbrio (skew)** grande e potencial gargalo. Eis a frase mais importante do módulo:

> **`lowerBound` e `upperBound` dividem o espaço de VALORES da `partitionColumn`; eles não recortam o dataset nem prometem a mesma quantidade de registros por partição.**

## 7. Filtrar e selecionar dados na origem

Uma consulta SQL usada no notebook e no desafio foi:

```sql
SELECT pedido_id, cliente_id, valor_total
FROM public.fato_pedidos_large
WHERE status = 'concluido'
```

A consulta foi passada como subquery aliased no parâmetro `table`. Assim, **PostgreSQL filtra as linhas e limita as colunas antes da transferência JDBC**. No laboratório, os 2 milhões viraram **500 mil linhas e apenas 3 colunas**. Com bounds baseados em `pedido_id` de 1 a 2 milhões, a distribuição foi:

```text
P0 125.000 | P1 125.000 | P2 124.999 | P3 125.001
```

Os **500 mil** são a contagem **após filtro**. Os IDs continuam espalhados pelo intervalo perto de 1–2 milhões; portanto **`upperBound=500_000` seria incoerente**, mesmo que a query retorne 500 mil linhas.

`WHERE` (ou outra restrição SQL adequada) é o mecanismo que limita dados. **Bounds são referências de paralelização.**

## 8. `fetchsize` e `batchsize`

Esses parâmetros não criam novas Spark Partitions:

| Configuração | Direção | Papel |
|---|---|---|
| `numPartitions` | Leitura JDBC | Quantidade de leituras/faixas paralelas |
| `fetchsize` | PostgreSQL → Spark | Número de linhas buscadas por lote pelo driver |
| `batchsize` | Spark → PostgreSQL | Quantidade de linhas agrupadas por lote de escrita |

No notebook, uma leitura com `"fetchsize": "100000"` manteve **4 partições**. No desafio, foi usado `"fetchsize": "1000"` e o resultado também continuou com quatro. Esses números são configurações experimentais, não receitas universais; lote muito grande pode exigir mais memória e pressionar o banco.

Na escrita, o código passou `"batchsize": "5000"` dentro das propriedades:

```python
properties={**credencias_banco, "batchsize": "5000"}
```

`**credencias_banco` **desempacota** os pares chave/valor do dicionário de conexão dentro de um novo dicionário, acrescido da configuração. Não altera o dicionário original.

## 9. `append`, `overwrite` e validação da carga

O laboratório gravou os pedidos concluídos em tabelas PostgreSQL e validou as contagens. Uma carga de **500.000** linhas seguida de `append` do mesmo DataFrame produziu **1.000.000**: não houve deduplicação automática. Em projetos reais, isso demanda estratégia de **idempotência** (chave única, `MERGE`/`ON CONFLICT`, staging e validação, conforme o desenho do pipeline).

**Alerta de operação:** `overwrite` via JDBC pode remover/recriar a tabela; se existem constraints, índices ou permissões geridos pelo banco, não use esse modo sem conferir a estratégia de escrita. No arquivo `CREATE TABLE public.sql`, por exemplo, `public.desafio_pedidos_concluidos` foi definido com `SERIAL PRIMARY KEY`, enquanto o desafio executou `overwrite` e depois conseguiu acrescentar novamente as mesmas IDs. Isso é compatível com a tabela ter sido recriada sem a constraint pela escrita do Spark. **Verifique sempre a definição real da tabela após um `overwrite`**: uma chave primária ainda existente rejeitaria IDs duplicados em vez de duplicá-los silenciosamente.

## O que foi feito no notebook da aula

Foi criado o Spark com driver JDBC, lido CSV e gravado Parquet, feita carga para `public.staging_pedidos` e leitura de volta para comparar tipos. A tabela grande foi lida primeiro com uma partição e depois com quatro, seguida de contagem por `spark_partition_id()`. Uma subquery com `WHERE status='concluido'` demonstrou redução de linhas e colunas antes de cruzar JDBC. Foram testadas opções `fetchsize` e escrita com `batchsize=5000`.

## O que foi feito no desafio 09

As partes A–C compararam uma leitura de **1 partição** com outra de **4 partições** e inspecionaram a distribuição. A parte D alterou propositalmente `upperBound` para 500 mil, produzindo 1.625.002 linhas na última partição. A parte E filtrou 500 mil pedidos concluídos ainda no PostgreSQL; F usou `fetchsize` sem alterar o paralelismo; G fez escrita usando **`batchsize=5000` tanto em `overwrite` quanto em `append` na versão enviada**; e H validou `IntegerType ↔ integer` e `DecimalType ↔ numeric`.

A resposta final propôs reduzir colunas e linhas na origem e escolher paralelismo JDBC, mas vale manter explícito um cuidado que não estava suficientemente destacado: **quatro, dez ou cem conexões JDBC também consomem recursos do PostgreSQL**. Mais conexões nem sempre trazem ganho; o número deve respeitar CPU, disco, rede, pool de conexões e consultas simultâneas de outros sistemas.

## Para revisar antes da Aula 10

```text
count de linhas NÃO é upperBound
bounds NÃO são WHERE
4 partições NÃO garantem 25% das linhas em cada uma
fetchsize NÃO é numPartitions
batchsize NÃO realiza deduplicação
Spark mais paralelo pode sobrecarregar a origem
```

**Próximo módulo:** Aula 10 — Performance (ainda não incluída nos ZIPs recebidos).
