# Aula 10 — Performance, diagnóstico e otimização no Apache Spark

> **Fontes analisadas:** [`aula_10_performance.ipynb`](../notebooks/aula_10_performance.ipynb) e [`desafio_10_performance.ipynb`](../desafios/desafio_10_performance.ipynb), extraídos do repositório **Spark Práticas**.  
> **Status:** Aula 10 estudada; **Desafio 10 concluído**. A documentação distingue os resultados registrados nos notebooks de explicações conceituais e recomendações técnicas adicionais.

A Aula 10 integrou os conhecimentos anteriores do curso: *Lazy Evaluation*, leitura de planos físicos, partições, transformações Narrow/Wide, Shuffle, agregações e estratégias de join. O foco passou a ser **identificar o que pode causar um gargalo, observar como o Spark executa a consulta e justificar uma otimização**, sem assumir que determinada função é sempre mais rápida.

---

## 1. Diagnóstico de performance: plano físico versus execução

O método `explain(True)` apresenta as etapas de planejamento de uma consulta Spark:

```text
Parsed Logical Plan
        ↓
Analyzed Logical Plan
        ↓
Optimized Logical Plan
        ↓
Physical Plan
```

No plano físico, operadores como `Exchange`, `Sort`, `HashAggregate`, `BroadcastExchange` e `SortMergeJoin` indicam **como o Spark pretende organizar o processamento**. A presença de `Exchange` permite identificar uma redistribuição planejada, mas não prova que ela seja o maior custo da aplicação.

O **Spark UI** complementa essa análise com métricas registradas em execução: duração de Tasks e Stages, Shuffle Read/Write, spill, Jobs e consultas SQL. Um Shuffle que transfere poucos bytes pode custar pouco; um estágio com Tasks muito desiguais pode exigir investigação de *Data Skew*.

**Princípio aplicado na aula:** antes de otimizar, identificar o operador, verificar seus efeitos e só então decidir se há um problema relevante.

## 2. Dataset-base e experimento Narrow versus Wide

O notebook `aula_10_performance.ipynb` inicia uma `SparkSession` com o nome `aula_10_performance` e gera um conjunto sintético com `spark.range()`:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

df_base = spark.range(
    start=0,
    end=2_000_000,
    step=1,
    numPartitions=8
)
```

Esse DataFrame tem **2.000.000 de IDs**, de `0` a `1.999.999`, originalmente distribuídos em **8 Spark Partitions**. O uso de `spark.range()` dispensa gerar um grande CSV para investigar particionamento e execução.

### 2.1. Filtro: transformação Narrow

O primeiro experimento preservou IDs divisíveis por dez:

```python
df_filtrado = df_base.filter(F.col("id") % 10 == 0)
```

**Resultados registrados:**

- `df_filtrado.count()` → **200.000 registros**.
- `df_filtrado.rdd.getNumPartitions()` → **8 partições**.
- `df_filtrado.explain(True)` → `Filter` sobre `Range`, **sem `Exchange` introduzido pelo filtro**.

O filtro é Narrow porque cada partição pode avaliar a expressão `(id % 10) == 0` e descartar registros localmente. Não é necessário reunir IDs com o mesmo valor nem obter uma ordenação global.

### 2.2. Agregação: resultados parciais e Shuffle

O segundo experimento cria cem grupos e conta registros por grupo:

```python
df_filtrado2 = df_base.withColumn("grupo", F.col("id") % 100)
df_agrupado = (
    df_filtrado2
    .groupBy("grupo")
    .count()
)
```

O notebook mostra **20.000 registros por grupo** nas dez linhas exibidas com `show(10)`. A distribuição é uniforme porque o intervalo contém exatamente 2 milhões de IDs e a chave é o resto da divisão por cem.

O plano físico registrado contém:

```text
HashAggregate (final)
    ↑
Exchange hashpartitioning(grupo, 200)
    ↑
HashAggregate (partial)
    ↑
Project: id % 100 → grupo
    ↑
Range: 2.000.000 registros / 8 partições
```

A execução do plano deve ser lida, conceitualmente, **de baixo para cima**:

1. `Project` calcula a chave `grupo`.
2. `HashAggregate partial` computa contagens intermediárias por grupo em cada partição de entrada.
3. `Exchange` redistribui **essas contagens parciais** pelas chaves de agrupamento.
4. `HashAggregate final` combina os resultados intermediários de cada chave.

A agregação parcial **não elimina necessariamente o Shuffle**: ela reduz o volume de informação enviado. Em um cenário idealizado com cem grupos presentes nas oito partições, a agregação local pode reduzir 2 milhões de linhas a aproximadamente **800 pares (grupo, contagem parcial)** antes do Shuffle. O valor ilustra a redução possível; não é uma medição de bytes do Spark UI.

## 3. `repartition()` versus `coalesce()`

O terceiro experimento reduz oito partições para quatro de duas maneiras:

```python
df_reparticionado = df_base.repartition(4)
df_coalesce = df_base.coalesce(4)
```

### `repartition(4)`

O plano apresenta:

```text
Optimized Logical Plan: Repartition 4, true
Physical Plan: Exchange RoundRobinPartitioning(4)
```

O `true` no plano lógico refere-se à solicitação de Shuffle. Nesse exemplo, o Spark redistribui os registros para formar quatro partições, normalmente com distribuição *round-robin*.

### `coalesce(4)`

O plano apresenta:

```text
Optimized Logical Plan: Repartition 4, false
Physical Plan: Coalesce 4
```

A redução ocorre, em geral, **combinando partições existentes sem um novo Shuffle completo**. Isso tende a reduzir o custo imediato, mas não garante a correção de desequilíbrios preexistentes.

O notebook verificou a distribuição com `F.spark_partition_id()`, `groupBy().count()` e `orderBy()`:

| Partição | `repartition(4)` | `coalesce(4)` |
|---|---:|---:|
| 0 | 500.000 | 500.000 |
| 1 | 500.000 | 500.000 |
| 2 | 500.000 | 500.000 |
| 3 | 500.000 | 500.000 |
| **Total** | **2.000.000** | **2.000.000** |

**Interpretação:** mesmo resultado e mesmo equilíbrio final **não significam planos de execução equivalentes**. `repartition()` pagou o custo de redistribuir; `coalesce()` aproveitou a distribuição já uniforme do `Range`. Em outros datasets, um Shuffle adicional pode compensar se melhorar substancialmente etapas posteriores.

> **Nota de interpretação:** o `groupBy()` usado para diagnosticar quantidades por partição também pode gerar operadores de Shuffle próprios; ele não deve ser confundido com o Shuffle do `repartition()` analisado.

## 4. Data Skew: diferença entre chave frequente e partição sobrecarregada

*Data Skew* é um **desequilíbrio na distribuição do trabalho** entre partições/Tasks. Um cenário comum ocorre quando milhões de linhas compartilham a mesma chave e precisam ser reunidas para determinada operação.

O notebook cria `df_skew` sem agregar os registros:

```python
df_skew = df_base.withColumn(
    "grupo",
    F.when(F.col("id") < 1_800_000, 0)
     .otherwise((F.col("id") % 9) + 1)
)
```

O DataFrame continua com **2.000.000 de registros**, dos quais:

- **1.800.000** possuem `grupo = 0` (90%).
- Os **200.000** restantes estão distribuídos pelos grupos `1` a `9`.

### 4.1. Particionamento por hash da chave

```python
df_skew_reparticionado = df_skew.repartition(8, "grupo")
```

O plano registra `Exchange hashpartitioning(grupo, 8)`. Como registros com o mesmo valor de `grupo` recebem o mesmo destino de hash, o grupo 0 fica concentrado em uma única partição. Outros grupos distintos podem acabar na mesma partição de destino.

**Distribuição observada no notebook da aula:**

| Partição | Registros |
|---|---:|
| 0 | 22.223 |
| 1 | 22.222 |
| 2 | 0 |
| 3 | 44.444 |
| 4 | 66.666 |
| **5** | **1.844.445** |
| 6 | 0 |
| 7 | 0 |
| **Total** | **2.000.000** |

As partições 2, 6 e 7 não aparecem nas contagens agrupadas porque ficaram vazias. A partição 5 recebeu **mais de 92%** das linhas. O excesso sobre 1,8 milhão ocorre porque outras chaves também foram direcionadas a ela.

### 4.2. Particionamento sem chave explícita

```python
df_skew_reparticionado2 = df_skew.repartition(8)
```

O `Physical Plan` apresenta `Exchange RoundRobinPartitioning(8)`, e a contagem por partição registrada mostrou **250.000 registros em cada uma das oito partições**.

A mesma base, com o mesmo número de partições, teve uma distribuição muito diferente porque mudou **a regra de distribuição**.

**Cuidado:** executar `repartition(8)` não é uma solução universal de Data Skew. Uma operação posterior pode exigir Shuffle por `grupo` e concentrar novamente as linhas da chave dominante.

### 4.3. Agregação por chave versus reparticionamento dos registros detalhados

Outro experimento usou:

```python
df_agregacao_skew = (
    df_skew
    .groupBy("grupo")
    .agg(F.count("grupo").alias("registros_grupo"))
    .orderBy("grupo")
)
```

O plano apresentou:

```text
Sort
    ↑
Exchange rangepartitioning(grupo, 200)    ← ordenação global
    ↑
HashAggregate final
    ↑
Exchange hashpartitioning(grupo, 200)   ← agregação
    ↑
HashAggregate partial
    ↑
Project / Range
```

A comparação esclareceu um dos pontos centrais do curso:

- `groupBy("grupo").count()` **pode reduzir milhões de registros a contagens parciais** antes da redistribuição.
- `repartition(8, "grupo")` **mantém as linhas detalhadas** e as redistribui pela chave; por isso, a chave dominante permanece concentrada.
- O segundo `Exchange` do primeiro plano foi introduzido por **`orderBy("grupo")`**, não é uma etapa obrigatória do `groupBy()`.

O impacto real do Skew depende, portanto, do **trabalho que resta após a redistribuição**. Uma contagem é diferente de um join que precisa preservar detalhes dos registros.

## 5. BroadcastHashJoin versus SortMergeJoin

Para comparar estratégias de join, a aula criou `df_dim_grupos`, uma dimensão com **dez linhas**, uma para cada valor de `grupo`, com a coluna `descricao`.

### 5.1. BroadcastHashJoin

```python
df_join = df_skew.join(
    F.broadcast(df_dim_grupos),
    on="grupo",
    how="inner"
)
```

O plano físico contém:

```text
BroadcastHashJoin [...], Inner, BuildRight
    ├── lado grande: Range / Project
    └── lado pequeno: BroadcastExchange → Scan ExistingRDD
```

`BuildRight` indica que a **estrutura hash é construída com o DataFrame à direita**, a dimensão pequena. O Spark disponibiliza esse lado aos executores e permite correspondências locais com os registros do lado grande, **sem um Shuffle por chave no lado grande para esse join**.

O notebook confirmou:

- `df_join.count()` → **2.000.000**.
- `df_join.filter(F.col("grupo") == 0).count()` → **1.800.000**.
- O join partindo de `df_skew` mostrou **8 partições com 250.000 registros cada**.

Esses dados **não foram eliminados nem deduplicados**. O join preservou os registros porque havia exatamente uma correspondência na dimensão para cada grupo. O motivo do equilíbrio é que `df_skew` já estava distribuído uniformemente e o Broadcast dispensou sua redistribuição por `grupo`.

> Broadcast **não corrige automaticamente** um DataFrame já desequilibrado. Ele pode evitar que o join introduza a redistribuição por chave que causaria o desequilíbrio.

### 5.2. SortMergeJoin

No notebook foi desabilitada temporariamente a seleção automática por Broadcast e usado um *hint* para obter a outra estratégia:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)

df_sort_merge_join = df_skew.join(
    df_dim_grupos.hint("merge"),
    on="grupo",
    how="inner"
)
```

O `explain("formatted")` registrou **`SortMergeJoin` com dois operadores `Exchange` e dois `Sort`** no plano apresentado:

```text
SortMergeJoin
    ├── Sort ← Exchange hashpartitioning(grupo, ...)
    └── Sort ← Exchange hashpartitioning(grupo, ...)
```

Essa estratégia pode custar mais quando as entradas ainda precisam ser redistribuídas e ordenadas. Chaves muito frequentes também podem criar partições mais pesadas. Ainda assim, `SortMergeJoin` pode ser adequado para tabelas grandes; não existe uma estratégia universalmente melhor.

**Observação de reprodutibilidade:** as células enviadas definem os thresholds de Broadcast para `-1`, mas não mostram a restauração posterior dos valores anteriores. Em novos experimentos, recomenda-se guardar e restaurar essas configurações para não influenciar comparações subsequentes. Essa é uma recomendação adicional, não uma etapa registrada como executada.

## 6. Adaptive Query Execution (AQE)

O **Adaptive Query Execution** permite que o Spark ajuste algumas decisões físicas durante a execução com base em estatísticas dos estágios já processados. Entre as possibilidades estão:

- Combinar partições de Shuffle pequenas para reduzir overhead de Tasks.
- Converter certas estratégias de join quando um lado real se mostra pequeno o suficiente.
- Tratar partições desiguais em alguns tipos de joins compatíveis.

Na Aula 10, a chamada:

```python
spark.conf.get("spark.sql.adaptive.enabled")
```

retornou **`'true'`**. Apareceram também planos com `AdaptiveSparkPlan isFinalPlan=false` e `isFinalPlan=true`.

- `false`: o plano adaptativo ainda não foi apresentado como finalizado naquela visualização.
- `true`: há um plano adaptativo final disponível para a consulta/execução representada.

**Limite da evidência:** o notebook confirma que o AQE estava habilitado, mas não demonstra, por si só, que uma conversão de join ou correção automática de Skew específica ocorreu naquela execução.

## 7. `cache()`, `persist()` e `unpersist()`

O Spark normalmente pode recomputar etapas de um DataFrame quando ele é reutilizado em diferentes ações ou ramificações. O cache evita parte desse retrabalho ao armazenar dados intermediários materializados.

O notebook realizou:

```python
df_cached = df_skew.cache()
df_cached.show(1)
df_cached.count()
```

- `cache()` marca o DataFrame para reutilização; **não materializa automaticamente todos os registros**.
- `show(1)` pode acessar apenas uma parte dos dados; **não garante preencher todo o cache**.
- `count()` faz uma ação que considera os registros e pode materializar as partições necessárias.

Na agregação posterior com `df_cached`, o plano físico apresentou:

```text
InMemoryTableScan
    ↑
InMemoryRelation
StorageLevel(disk, memory, deserialized, 1 replicas)
```

Essa é a evidência de que a consulta lê o DataFrame armazenado, em vez de depender exclusivamente da recomputação do `Range` e do `Project` originais.

Ao terminar, a aula executou `df_cached.unpersist()` para liberar os dados persistidos.

**Quando considerar cache:** processamento intermediário caro reutilizado diversas vezes, com armazenamento disponível. **Quando evitar:** dados utilizados uma única vez ou custo de armazenamento/materialização maior que o trabalho economizado. `persist()` permite controlar o nível de armazenamento conforme o caso.

## 8. Spark UI: quando o Shuffle foi um gargalo de verdade?

O notebook inspecionou `spark.sparkContext.uiWebUrl` e disparou uma agregação para gerar Jobs e Stages. No registro extraído do ZIP, a URL retornou um hostname interno do container na **porta 4041**, após mensagem de que a porta 4040 já estava ocupada. Portanto, **4040 é o padrão, não uma garantia**; o endereço efetivo depende da SparkSession e da publicação/encaminhamento das portas Docker.

O Spark UI permite investigar:

| Área | Evidência relevante |
|---|---|
| **Jobs** | Ações que dispararam execução e seus Stages |
| **Stages** | Duração e distribuição das Tasks, Shuffle Read/Write, spill |
| **SQL / DataFrame** | Operadores e métricas associados às consultas |
| **Storage** | Dados efetivamente armazenados em cache |
| **Executors** | Uso de recursos e Tasks dos executores |

Um `Exchange` no `explain(True)` indica a redistribuição no plano. Só uma inspeção das métricas pode ajudar a responder **quanto foi movimentado, quanto demorou e se houve gargalo observável**. Mesmo no Spark UI, é necessário interpretar contexto: uma Task lenta, sozinha, não prova que Shuffle foi a causa.

---

# O que foi feito no notebook da Aula 10

O notebook [`aula_10_performance.ipynb`](../notebooks/aula_10_performance.ipynb) possui **55 células de código** na versão enviada. A sequência prática registrada foi:

| Etapa | Implementação e verificação | Resultado/evidência |
|---|---|---|
| Fundamentos de performance | `spark.range`, filtro por módulo, `count`, partições, `explain(True)` | 2 mi → 200 mil; 8 partições; `Filter` sem novo `Exchange` |
| Agregações | `grupo = id % 100`, `groupBy().count()` | 20 mil por grupo; `HashAggregate partial` / `Exchange` / final |
| Particionamento | `repartition(4)` e `coalesce(4)` | Ambas com 500 mil por partição; planos diferentes |
| Data Skew | `grupo=0` para 1,8 mi; `repartition(8, "grupo")` | Partição 5 com 1.844.445 linhas |
| Distribuição sem chave | `repartition(8)` | 250 mil por partição |
| Agregação sobre Skew | `groupBy("grupo").count().orderBy("grupo")` | Agregação parcial e dois Exchanges com finalidades diferentes |
| Broadcast | `F.broadcast(df_dim_grupos)` | `BroadcastHashJoin`, `BuildRight`, 2 mi preservados |
| SortMerge | `hint("merge")` e thresholds de Broadcast desabilitados | `SortMergeJoin`, Exchanges e Sorts |
| AQE | Consulta à configuração | `spark.sql.adaptive.enabled = 'true'` |
| Cache | `cache`, ações, `explain`, `unpersist` | `InMemoryTableScan` / `InMemoryRelation` |
| Spark UI | `uiWebUrl` e execução de agregação | Sessão Spark UI identificada; investigação por Jobs/Stages |

A sequência foi utilizada para relacionar as escolhas de API aos operadores físicos observados, sem reduzir performance a simples comparação de tempo de uma célula do notebook.

---

# O que foi feito no Desafio 10 — Diagnóstico e otimização

> **Fonte:** [`desafio_10_performance.ipynb`](../desafios/desafio_10_performance.ipynb). O desafio contém **48 células**, incluindo código e respostas em Markdown. A correção final do curso o considerou **aprovado**; a evidência de performance medida permaneceu parcial.

## Parte A — Preparação e diagnóstico

O desafio recriou `df_base` com 2 milhões de IDs, `df_skew` com a concentração proposital de `grupo=0` e `df_dim_grupos` com dez linhas e descrições.

O estudante inspecionou contagem, schema, distribuição dos grupos e duas abordagens de reparticionamento:

```python
df_skew_repartition = df_skew.repartition(8, "grupo")
df_skew_repartition2 = df_skew.repartition(8)
```

**Resultados verificados:**

- `df_skew.count()` → **2.000.000**.
- Grupo 0 → **1.800.000**.
- Por chave → partição 5 com **1.844.445** registros, outras pequenas ou vazias.
- Sem chave → **250.000** registros em cada uma das oito partições.

**Aprendizado demonstrado:** número de partições é insuficiente para diagnosticar equilíbrio; é necessário analisar a distribuição das chaves e dos registros após o particionamento.

## Parte B — Comparação de estratégias de join

Foram produzidos dois `inner joins` entre `df_skew` e `df_dim_grupos`:

```python
# Versão Broadcast
F.broadcast(df_dim_grupos)

# Versão SortMerge
# dica/hint de merge, sem Broadcast explícito
```

As duas versões retornaram **2.000.000 de registros**. O desafio também produziu a **mesma agregação por `descricao`**, permitindo verificar o resultado analítico nos dois caminhos:

| Descrição | Registros |
|---|---:|
| Grupo 0 | 1.800.000 |
| Grupo 1 | 22.223 |
| Grupo 2 | 22.223 |
| Grupos 3–9 | 22.222 por grupo |

Os planos registrados mostraram:

- **Broadcast:** `BroadcastExchange` e `BroadcastHashJoin` (`BuildRight`).
- **SortMerge:** `Exchange` e `Sort` nos dois lados, seguidos de `SortMergeJoin`.

Na resposta em Markdown, foi escolhida a estratégia Broadcast por causa da dimensão de dez registros e da possibilidade de evitar Shuffle por chave do lado grande. Essa escolha é **coerente com a estrutura da consulta**, mas não constitui uma medição isolada de ganho de velocidade.

## Parte C — Cache e AQE

O desafio comparou duas análises sobre a base sem cache e depois com cache:

1. Contagem de registros por `grupo`.
2. Contagem filtrada do `grupo = 3`.

A versão com cache materializou o DataFrame com `count()`, reutilizou-o nas consultas e aplicou `unpersist()` ao final.

**Resultados/evidências:**

- Ambas as variantes mantiveram as mesmas contagens; `grupo = 3` apresentou **22.222 registros**.
- Os planos com cache continham `InMemoryTableScan` e `InMemoryRelation`.
- A configuração `spark.sql.adaptive.enabled` retornou **`'true'`**.

A resposta do desafio justificou cache para reutilização de DataFrames e destacou que o AQE não substitui modelagem correta, planejamento de joins, filtros ou interpretação de custos reais.

## Parte D — Diagnóstico com Spark UI

O notebook registrou em Markdown uma consulta de agregação da Parte B:

| Informação registrada | Valor |
|---|---|
| Identificador | **SQL Query ID 23** |
| Duração informada | **1 segundo** |
| Jobs associados | **68, 69 e 70** |

Na conclusão escrita pelo estudante, também foi mencionado um `HashAggregate` com **spill zero** na inspeção realizada.

**Limitação explicitada no próprio desafio:** não foram registradas métricas completas e equivalentes do SortMergeJoin (tempo total, Shuffle Read/Write, duração de Tasks e spill) para uma comparação quantitativa rigorosa entre as estratégias.

Por isso, o diagnóstico final foi cuidadoso: **BroadcastHashJoin parece mais adequado à topologia e ao tamanho das tabelas, e o Physical Plan explica por quê; os dados disponíveis não comprovam de forma conclusiva que ele foi mais rápido naquela execução**.

Essa distinção entre **evidência de plano**, **métrica observada** e **hipótese de melhoria** foi um dos aprendizados centrais do desafio.

---

## 9. Erros de interpretação evitados ao longo da aula

1. **`Exchange` não é sinônimo de gargalo:** ele descreve uma redistribuição; é preciso medir volume e duração.
2. **`HashAggregate partial` não elimina todo Shuffle:** reduz os dados transferidos quando a operação admite agregação local.
3. **Mais partições não garantem maior velocidade:** recursos disponíveis, overhead e equilíbrio também importam.
4. **`coalesce()` não resolve necessariamente Skew:** juntar partições sem redistribuição pode preservar desequilíbrios.
5. **`repartition(n)` e `repartition(n, chave)` são estratégias diferentes:** a segunda mantém a mesma chave no mesmo destino de hash.
6. **Broadcast não deduplica nem remove registros:** quando o lado pequeno possui uma correspondência por chave, a cardinalidade do lado grande pode permanecer inalterada.
7. **Broadcast não conserta automaticamente dados já desequilibrados:** pode evitar o Shuffle que concentraria uma chave em um join.
8. **AQE habilitado não prova otimização específica aplicada:** a confirmação depende do plano final e das métricas.
9. **Cache não é grátis:** a materialização ocupa recursos e só tende a compensar se houver reutilização suficiente.
10. **Melhor escolha arquitetural não equivale a benchmark conclusivo:** planos e métricas devem ser apresentados separadamente.

## 10. Síntese da Aula 10

O fluxo de decisão consolidado foi:

```text
Identificar a operação e a necessidade do resultado
                     ↓
Inspecionar o Optimized Logical Plan e Physical Plan
                     ↓
Localizar Exchange, Sort, HashAggregate, Join, Scan e Cache
                     ↓
Avaliar distribuição, cardinalidade e frequência das chaves
                     ↓
Consultar Spark UI: tempo, Tasks, Shuffle, spill e execução
                     ↓
Escolher e justificar uma mudança específica
                     ↓
Comparar as evidências antes/depois em condições semelhantes
```

**Conclusão:** a Aula 10 encerrou a etapa de estudo de performance mostrando que compreender o Spark não significa apenas conhecer funções: significa reconhecer seus custos, evitar conclusões precipitadas e saber distinguir o que foi **planejado**, o que foi **executado** e o que foi **efetivamente comprovado**.

**Próxima etapa do curso:** [Desafio Final Integrador](../desafios/desafio_11_final.ipynb), reunindo os conteúdos das dez aulas em uma avaliação única.
