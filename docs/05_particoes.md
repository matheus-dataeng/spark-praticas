# Aula 05 — Partições, Narrow, Wide, Exchange e Shuffle

> **Fontes:** [`aula_05_particoes.ipynb`](../notebooks/aula_05_particoes.ipynb) e [`desafio_05_particoes.ipynb`](../desafios/desafio_05_particoes.ipynb).

## 1. O que são Spark Partitions?

Um DataFrame é processado de forma distribuída por **partições**: unidades lógicas de dados usadas para planejar trabalho paralelo. O Spark cria *Tasks* para processar essas partições, e os *Executors* executam as Tasks dentro dos recursos disponíveis.

```text
DataFrame
├── Spark Partition 0 → Task 0 ┐
├── Spark Partition 1 → Task 1 ├→ Executors / CPU
├── Spark Partition 2 → Task 2 ┤
└── Spark Partition 3 → Task 3 ┘
```

**Uma partição não é um DataFrame separado, e uma partição não é um executor.** Quatro partições tampouco garantem quatro Tasks simultâneas; isso depende da capacidade disponível.

O notebook começou lendo `05_particoes/clientes.csv` e consultando:

```python
df.rdd.getNumPartitions()
```

No desafio, a leitura inicial retornou **1 partição**. Esse resultado é específico do arquivo/ambiente: não é regra geral de CSV nem de Spark.

## 2. Narrow x Wide: a distinção central

**Narrow transformation:** cada partição consegue gerar sua saída a partir de dependências locais, sem exigir redistribuição dos registros entre partições. `select`, `filter` e `withColumn` são os exemplos vistos no curso.

**Wide transformation:** exige reagrupamento, redistribuição ou dependências entre partições, frequentemente criando um Shuffle. `repartition`, agregações por chave e ordenações globais são exemplos.

```text
NARROW                         WIDE
P0 → filter → P0               P0 ───┬──→ nova P0
P1 → filter → P1               P1 ───┼──→ nova P1
P2 → filter → P2               P2 ───┴──→ nova P2
                                      Shuffle
```

**Correção de uma dúvida do desafio:** uma operação não é Narrow *porque existe apenas uma partição*. Ela é Narrow por suas **dependências entre partições**. `filter` continua Narrow com uma ou mil partições. `repartition` continua sendo uma operação tipicamente Wide mesmo se, num caso trivial, pouco dado precisar se mover.

## 3. `repartition()` e `Exchange`

O notebook criou:

```python
df_repartition = df.repartition(4)
```

Depois verificou o número de partições e inspecionou `explain(True)`. A intenção é redistribuir o DataFrame para quatro partições, introduzindo, no plano, algo como:

```text
Exchange RoundRobinPartitioning(4)
   ↓
4 partições de saída
```

**Exchange** é o operador de execução que estabelece a distribuição desejada; **Shuffle** é a redistribuição física dos dados associada a essa fronteira. Nenhum dos dois é uma agregação por si só. Uma redistribuição desnecessária pode causar custo de rede, serialização, disco e novas Tasks.

## 4. `coalesce()` e redução de partições

O exemplo seguinte partiu de `df_repartition` e executou:

```python
df_coalesce = df_repartition.coalesce(2)
```

O objetivo é **reduzir** o número de partições, normalmente sem uma redistribuição completa adicional. No plano pode aparecer `Coalesce 2` sobre o plano anterior — que ainda contém um `Exchange` criado pelo `repartition(4)`.

Essa é uma nuance importante: **a presença de `Exchange` no plano de um DataFrame após `coalesce()` não significa necessariamente que o próprio `coalesce()` introduziu novo Shuffle**; o Exchange pode pertencer a uma transformação anterior.

## 5. `AdaptiveSparkPlan`

Nos planos analisados também pode aparecer `AdaptiveSparkPlan isFinalPlan=false`, vinculado a mecanismos de otimização adaptativa (AQE) durante a execução. Esse wrapper não é sinônimo de Shuffle e não surge apenas porque `repartition()` foi usado. O curso introduziu sua presença, reservando a discussão mais prática de otimização para Performance.

## O que foi feito no notebook da aula

Foi medido o particionamento inicial do CSV, analisado seu plano, criado `df.repartition(4)` com nova leitura de `getNumPartitions()` e `explain(True)`, e depois executado `coalesce(2)` com a mesma inspeção. O contraste central foi: **redistribuir** versus **reduzir sem redistribuir completamente**.

## O que foi feito no desafio 05

O desafio construiu um DataFrame de clientes selecionando `cliente_id`, `nome`, `renda_mensal`, `ativo`, `cidade`, `estado`, filtrando ativos, criando `renda_anual` e uma classificação textual por cidade (`Soteropolitano`, `Feirense` ou `Paulista` conforme o exemplo). Depois foram comparadas as três situações:

| Etapa | Partições verificadas | Operação relevante |
|---|---:|---|
| Leitura inicial | 1 | CSV |
| `df_resultado.repartition(2)` | 2 | Redistribuição; `Exchange RoundRobinPartitioning` |
| `df_resultado_partition.coalesce(1)` | 1 | Redução do número de partições |

As respostas identificaram o Shuffle no `repartition`, a Action `show()` e o risco de reduzir uma carga enorme a **uma única partição** apenas para obter um arquivo. Também houve uma analogia de lotes de estoque e funcionários para ilustrar a redistribuição do trabalho.

**Refinamentos do desafio:**

- `select`/`filter`/`withColumn` são Narrow por sua semântica, não pelo DataFrame originalmente conter uma única partição.
- `coalesce(1)` pode levar a escrita de um único arquivo de dados, mas gera gargalo e reduz o paralelismo; isso não deve virar padrão para grandes volumes.
- `show()` dispara o cálculo necessário à visualização; `getNumPartitions()` revela a estrutura de partições, podendo acionar planejamento sem corresponder a processar todas as linhas da consulta.

## Recapitulação visual

```text
FileScan
   ↓
select / filter / withColumn  (Narrow)
   ↓
Exchange RoundRobinPartitioning(2)  (Wide; Shuffle)
   ↓
Coalesce 1  (redução; normalmente sem novo Shuffle completo)
   ↓
show()  (Action)
```

**Próximo módulo:** [06 — Agregações](06_agregacoes.md).
