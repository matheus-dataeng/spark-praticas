# Aula 06 — Agregações e `HashAggregate` no Physical Plan

> **Fontes:** [`aula_06_agregacoes.ipynb`](../notebooks/aula_06_agregacoes.ipynb) e [`desafio_06_agregacoes.ipynb`](../desafios/desafio_06_agregacoes.ipynb).

## 1. Agrupar significa reunir dados pela chave

O notebook utilizou `06_agregacoes/vendas.csv`. Uma agregação agrupada responde a uma pergunta por **chave de negócio**, por exemplo faturamento por `categoria`:

```python
df_agregado = (
    df.groupBy("categoria")
      .agg(F.sum("valor_total").alias("faturamento_total"))
)
```

O resultado contém **uma linha por categoria**. Para somar corretamente, o Spark precisa combinar registros da mesma chave, que podem estar em partições distintas. Esse requisito torna `groupBy()` tipicamente **Wide**.

## 2. O fluxo físico mais importante do módulo

Um plano clássico de agregação distribuída é:

```text
FileScan (lê colunas necessárias)
               ↓
HashAggregate partial
               ↓
Exchange hashpartitioning(chave, ...)
               ↓
     Shuffle entre partições
               ↓
HashAggregate final
               ↓
      resultado por chave
```

**HashAggregate partial** calcula estados de agregação locais. Em vez de transmitir todas as vendas individuais para a etapa seguinte, Spark pode transmitir somas/contagens parciais por chave. Isso frequentemente reduz muito o volume de dados enviado no Shuffle.

**Exchange** estabelece a redistribuição por chave (`hashpartitioning`). **Shuffle** é o movimento dos dados. **HashAggregate final** reúne os estados parciais para obter o resultado definitivo. Não diga que `Exchange` ou `Shuffle` somam valores: **quem agrega é o `HashAggregate`**.

### Exemplo passo a passo

```text
P0: BA = 100, SP = 50
P1: BA = 200, RJ = 80

HashAggregate partial:
P0: (BA,100), (SP,50)
P1: (BA,200), (RJ,80)

Exchange / Shuffle: faz os estados de BA chegarem à mesma distribuição lógica

HashAggregate final:
BA = 300 | SP = 50 | RJ = 80
```

A representação é didática; o plano real pode incluir operadores adicionais por tipo de agregação, cardinalidade e AQE.

## 3. Múltiplas métricas no mesmo agrupamento

No notebook foi produzido um resumo por `estado` com `sum`, `avg`, `count`, `max` e `min`:

```python
df_agregado2 = df.groupBy("estado").agg(
    F.sum("valor_total").alias("faturamento_total"),
    F.avg("valor_total").alias("ticket_medio"),
    F.count("*").alias("quantidade_vendas"),
    F.max("valor_total").alias("maior_venda"),
    F.min("valor_total").alias("menor_venda"),
)
```

Quando as métricas compartilham as mesmas chaves de agrupamento, o Spark costuma **compartilhar a redistribuição necessária**, em vez de criar um Shuffle independente para cada soma, média ou contagem. A média pode ser representada internamente por **soma e contagem parciais**, combinadas ao final.

## 4. Chave composta x agregação global

O notebook também agrupou por **`estado` e `forma_pagamento`**, produzindo uma linha por combinação das duas dimensões. No plano, a distribuição pode usar `hashpartitioning(estado, forma_pagamento, ...)`.

A agregação global foi feita com `df.agg(...)`, sem `groupBy`:

```python
df_global = df.agg(
    F.sum("valor_total").alias("faturamento_total"),
    F.avg("valor_total").alias("valor_medio"),
    F.count("*").alias("quantidade_total"),
)
```

Como há um único resultado final e nenhuma chave, o plano pode introduzir **`Exchange SinglePartition`** para consolidar os estados parciais. Isso não significa que toda a leitura anterior tenha ocorrido em apenas uma partição.

## 5. Agregações condicionais

No notebook foi construído faturamento e quantidade de pagamentos por Pix, além de faturamento de Salvador:

```python
F.sum(F.when(F.col("forma_pagamento") == "Pix", 1).otherwise(0))
F.sum(F.when(F.col("forma_pagamento") == "Pix", F.col("valor_total")).otherwise(0))
```

Para **contagem condicional**, `sum(when(cond, 1).otherwise(0))` é a forma coerente: `count()` conta valores não nulos, **inclusive zero**, então `count(when(...).otherwise(0))` não produziria a contagem de registros que atendem à condição.

## 6. Por que `countDistinct()` pode exigir mais etapas?

O notebook calculou `countDistinct("vendedor_id")` por `estado`. Um valor repetido pode aparecer em mais de uma partição; por isso, **somar contagens distintas locais pode contar o mesmo vendedor várias vezes**. O plano pode usar deduplicação distribuída por `(estado, vendedor_id)` e, depois, nova agregação por `estado`, com etapas adicionais de `HashAggregate` e `Exchange`.

## O que foi feito no notebook da aula

A sequência foi: faturamento por categoria; painel por estado com cinco métricas; agrupamento composto por estado e meio de pagamento; resumo global; métricas condicionais para Pix/Salvador; e número de vendedores distintos por estado. Cada etapa principal teve `show()` e `explain(True)` para relacionar métricas calculadas aos operadores físicos.

## O que foi feito no desafio 06

O desafio construiu um **resumo por estado** com quantidade de vendas, faturamento, ticket médio, `countDistinct(vendedor_id)` e métricas específicas de Pix. Também elaborou resumos por combinações de chaves e global, e respondeu perguntas sobre o significado de `FileScan`, `HashAggregate partial`, `Exchange/Shuffle`, `HashAggregate final` e `SinglePartition`.

Nas respostas do desafio, a explicação do parcial inicialmente enfatizou economia de memória; a definição mais precisa é **agregar localmente para reduzir o volume de dados que pode atravessar o Shuffle**. E o `HashAggregate final` **combina estados parciais**, não percorre necessariamente todas as linhas originais novamente.

## Para revisar

```text
Partial → agrega localmente
Exchange/Shuffle → redistribui pela chave
Final → combina os parciais
```

`countDistinct` demanda cuidado extra por duplicidades espalhadas entre partições. Uma agregação global não tem chave e pode precisar de `SinglePartition` na consolidação final.

**Próximo módulo:** [07 — Joins](07_joins.md).
