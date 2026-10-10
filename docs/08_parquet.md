# Aula 08 — Parquet, leitura seletiva e particionamento no storage

> **Fontes:** [`aula_08_parquet.ipynb`](../notebooks/aula_08_parquet.ipynb) e [`desafio_08_parquet.ipynb`](../desafios/desafio_08_parquet.ipynb).

## 1. Por que converter CSV para Parquet?

**CSV** armazena texto separado por delimitadores. Não incorpora os tipos de dados da mesma forma que um arquivo colunar tipado: ao ler, o Spark pode precisar de um schema fornecido ou de `inferSchema=True` para interpretar números, booleanos e datas.

**Parquet** é binário, **colunar**, mantém metadados de schema e suporta compressão. Isso permite que o leitor busque seletivamente colunas utilizadas na consulta e, em condições favoráveis, evite blocos sem registros relevantes.

```text
CSV:     linha1 [id, cliente, status, valor] | linha2 [...] | ...
Parquet: coluna id [...], coluna cliente [...], coluna valor [...]
```

Essa figura é conceitual: Parquet agrupa dados por **Row Groups**, e dentro de cada um armazena colunas em *column chunks*; ele não coloca necessariamente todos os valores de uma coluna do dataset inteiro em um único bloco.

## 2. Experimento: escrita e releitura

O notebook leu `/workspace/datasets/08_parquet/pedidos.csv` e exibiu um schema com `pedido_id`, `cliente_id`, `data_pedido`, `status`, `canal` e `valor_total`. Em seguida:

```python
df_csv.write.mode("overwrite").parquet(
    "/workspace/datasets/08_parquet/arquivos_parquet"
)
df_parquet = spark.read.parquet(
    "/workspace/datasets/08_parquet/arquivos_parquet"
)
df_parquet.printSchema()
```

O schema voltou com os tipos correspondentes sem `inferSchema=True`, porque o Parquet mantém metadados sobre os tipos. Observe que `write.parquet("caminho")` normalmente cria **um diretório de dataset com arquivos `part-*`**, e não um arquivo único nomeado exatamente como o diretório. A leitura correta do dataset é pela **pasta raiz**, não por apenas um arquivo `part-00000`.

No desafio, o arquivo foi escrito explicitamente com **Snappy**:

```python
df_pedidos.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("/workspace/datasets/08_parquet/pedidos_parquet_desafio")
```

Snappy prioriza boa velocidade de compressão/descompressão, com economia de bytes de armazenamento e I/O. Melhor compressão não implica automaticamente melhor desempenho em todas as consultas, pois entram na conta CPU, tamanho e organização dos dados.

## 3. Column Pruning: escolher as colunas necessárias

A operação:

```python
df_parquet.select("cliente_id", "valor_total").explain(True)
```

mostrou no `FileScan parquet` um `ReadSchema` apenas com `cliente_id` e `valor_total`. A fonte tinha seis colunas, mas a operação precisava de duas.

```text
ReadSchema: struct<cliente_id:int,valor_total:double>
```

**Column Pruning** é a redução de colunas necessárias à leitura. Em Parquet, seu armazenamento colunar permite economizar bytes de forma mais efetiva do que em uma fonte textual de linhas completas.

## 4. Predicate Pushdown: filtro próximo ao leitor

O notebook filtrou `valor_total > 500` e inspecionou `explain(True)`:

```python
df_pushdown = (
    df_parquet.select("cliente_id", "valor_total")
              .filter(F.col("valor_total") > 500)
)
```

No plano, podem aparecer:

```text
DataFilters:   [isnotnull(valor_total), valor_total > 500]
PushedFilters: [IsNotNull(valor_total), GreaterThan(valor_total, 500.0)]
ReadSchema:    struct<cliente_id:int,valor_total:double>
```

`DataFilters` registra as condições de dados presentes na leitura/processamento. `PushedFilters` mostra condições enviadas ao leitor. **Um filtro listado em `PushedFilters` não garante que o Parquet pulou Row Groups específicos**; indica que o leitor pode explorá-lo, dependendo das estatísticas e dados.

## 5. Row Groups e Data Skipping

Um Parquet pode ter vários **Row Groups**, cada um contendo informações sobre colunas e, quando disponíveis, estatísticas como mínimo/máximo.

```text
Row Group A: max(valor_total)=300
Row Group B: max(valor_total)=900
Row Group C: max(valor_total)=400
Filtro: valor_total > 500
```

Se as estatísticas forem confiáveis, os grupos A e C podem ser ignorados, pois nem seu máximo atende ao filtro. O B **precisa ser considerado**, mas isso não significa que todas as suas linhas sejam válidas.

```text
Predicate Pushdown → condição chega ao leitor
Data Skipping       → leitor pode ignorar blocos impossíveis por metadados
```

São conceitos relacionados, mas **não idênticos**. E Row Group é **bloco interno do Parquet**, não é Spark Partition.

## 6. `partitionBy()` e diretórios físicos

O notebook escreveu:

```python
df_csv.write.mode("overwrite") \
    .partitionBy("status") \
    .parquet("/workspace/datasets/08_parquet/pedidos_por_status")
```

A organização física do dataset passa a usar os valores da coluna:

```text
pedidos_por_status/
├── status=concluido/
├── status=enviado/
├── status=cancelado/
└── ...
```

Ao reler a pasta raiz, o DataFrame ainda oferece a coluna `status`, mas no `ReadSchema` do arquivo ela pode **não aparecer**: o Spark reconstrói seu valor do nome do diretório `status=...`.

## 7. Partition Pruning

Quando se aplica:

```python
df_pedidos_status.filter(F.col("status") == "concluido")
```

foi observado no Physical Plan:

```text
PartitionFilters: [isnotnull(status), status = concluido]
PushedFilters: []
```

**Partition Pruning** permite eliminar da leitura as partições físicas que não atendem à condição, sem precisar processar seus arquivos de dados. O mecanismo pode ainda examinar nomes de diretórios/metadados para determinar quais partições existem.

## 8. Três “partições” diferentes

| Conceito | Onde existe | Para que serve |
|---|---|---|
| **Row Group** | Dentro do arquivo Parquet | Organização colunar por bloco; metadados e possível data skipping |
| **Spark Partition** | Plano de execução/DataFrame | Unidade de paralelismo; Tasks e Executors |
| **Partição de dataset (`partitionBy`)** | Diretórios/arquivos no storage | Partition Pruning por valores das colunas |

**`repartition()` não é `write.partitionBy()`**. O primeiro redistribui **partições de execução** (normalmente com Shuffle); o segundo organiza **arquivos de saída** por valores de colunas. Podem ser usados no mesmo pipeline, mas resolvem problemas diferentes.

## 9. Small files e escolha de particionamento

No fim do notebook há uma tentativa de escrita por `cliente_id` **comentada**, acompanhada do aviso sobre cardinalidade muito alta. Uma coluna com milhões de valores distintos pode gerar milhares/milhões de diretórios ou combinações físicas, com pouco dado em cada um. Isso aumenta o risco de **small files**, pressão sobre metadados e overhead de planejamento.

Não é correto afirmar que um valor distinto gera obrigatoriamente **um único arquivo**: podem existir múltiplos arquivos em uma partição física, dependendo das Tasks de escrita. E **baixa cardinalidade sozinha não basta**; uma boa partição física também precisa representar filtros frequentes.

## O que foi feito no notebook da aula

Foi realizada a conversão CSV → Parquet e releitura do schema; uma seleção de `cliente_id` e `valor_total` para demonstrar `ReadSchema`; um filtro `valor_total > 500` para observar `PushedFilters`; duas escritas de pedidos (normal e por `status`); e a análise de `PartitionFilters` no filtro `status='concluido'`. Por fim, o notebook registra explicitamente o risco de usar `cliente_id` como partição física.

## O que foi feito no desafio 08

As partes A–F reproduziram o fluxo de conversão com **Snappy explícito**, Column Pruning, Predicate Pushdown, escrita particionada, Partition Pruning e as três otimizações no mesmo plano. A parte G respondeu corretamente que Row Groups A e C poderiam ser descartados por `max < 500`. Na parte H, o exercício pediu o particionamento de vendas consultadas frequentemente por **ano e mês**: a resposta do notebook também mencionou **estado**. Tecnicamente, `ano/mes` é a escolha mais justificada pelo padrão de consultas fornecido; `estado` só deveria ser acrescentado com evidência de consultas que filtram por ele e volumes razoáveis por partição.

A questão final comparou `CSV → Spark → CSV` com `CSV → Spark → Parquet → consultas posteriores`, justificando a vantagem do segundo em datasets analíticos reutilizados, pela combinação de schema, compressão, leitura colunar e pruning.

## Checklist de interpretação do plano

```text
ReadSchema       → quais COLUNAS realmente preciso ler?
PushedFilters    → quais FILTROS chegaram ao leitor?
PartitionFilters → quais PARTIÇÕES FÍSICAS posso ignorar?
Row Group stats  → quais BLOCOS internos talvez possa pular?
```

**Próximo módulo:** [09 — JDBC e PostgreSQL](09_jdbc_postgresql.md).
