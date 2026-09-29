# Spark Práticas — Apache Spark para Engenharia de Dados

Repositório de estudos práticos de **Apache Spark / PySpark** com foco em **Engenharia de Dados**.

O objetivo deste projeto não é apenas aprender a sintaxe da API de DataFrames, mas entender **como o Spark processa os dados internamente**, como decisões de particionamento e estratégias físicas impactam a execução e como usar Spark de forma consciente em pipelines reais.

> **Foco principal:** utilizar Spark como motor de processamento para ingestão, integração e preparação técnica de grandes volumes de dados, mantendo transformações analíticas e regras de negócio em ferramentas mais adequadas, como **dbt**, quando fizer sentido na arquitetura.

---

## 🎯 Objetivos

Ao longo das aulas e desafios, o repositório trabalha:

- DataFrames e imutabilidade;
- Lazy Evaluation;
- Lineage, DAG e Catalyst Optimizer;
- leitura do `explain(True)` e do Physical Plan;
- transformações Narrow e Wide;
- partições, Tasks, Executors e paralelismo;
- `repartition()` e `coalesce()`;
- Shuffle e `Exchange`;
- agregações e `HashAggregate`;
- cardinalidade e qualidade de dados;
- joins e multiplicação de linhas;
- `left_semi` e `left_anti`;
- `BroadcastHashJoin`;
- `SortMergeJoin`;
- CSV, Parquet e JDBC;
- princípios básicos de performance em Spark.

A ideia é sair de:

```text
"Eu sei escrever operações em PySpark"
```

para:

```text
"Eu consigo entender o que o Spark está fazendo com os dados
e identificar os impactos da minha implementação."
```

---

## 🧠 Metodologia

Cada módulo segue uma abordagem progressiva:

```text
Teoria
  ↓
API / Sintaxe
  ↓
Exemplos simples
  ↓
Physical Plan / explain(True)
  ↓
Exercícios
  ↓
Desafio prático
```

O foco é escrever o código manualmente e usar o plano de execução para entender o comportamento do Spark, evitando apenas memorizar funções.

---

## 📚 Conteúdo

### 01 — Fundamentos
- `SparkSession`
- leitura de CSV
- schema
- `select`
- `filter`
- `withColumn`
- transformations x actions
- Lazy Evaluation
- imutabilidade

### 02 — Lineage e DAG

```text
Parsed Logical Plan
        ↓
Analyzed Logical Plan
        ↓
Optimized Logical Plan
        ↓
Physical Plan
```

- Lineage
- DAG
- Catalyst Optimizer
- `explain(True)`
- comparação entre diferentes implementações e planos otimizados

### 03 — Transformações
- `select`
- `drop`
- `withColumn`
- `withColumnRenamed`
- `alias`
- Column Pruning

### 04 — Filtros e ordenação
- operadores lógicos
- múltiplas condições
- `filter`
- `orderBy`
- `limit`
- impacto de ordenações globais

### 05 — Partições

```text
DataFrame
├── Partição 1
├── Partição 2
├── Partição 3
└── Partição N
```

Principais conceitos:

- partições
- Tasks
- Executors
- paralelismo
- `repartition`
- `coalesce`
- Narrow x Wide
- Exchange
- Shuffle

Regra mental:

```text
Partições dividem os dados.
Tasks processam as partições.
Executors executam as Tasks.
```

### 06 — Agregações

```text
HashAggregate parcial
        ↓
Exchange / Shuffle
        ↓
HashAggregate final
```

- `groupBy`
- `agg`
- `sum`
- `avg`
- `count`
- `countDistinct`
- agregações condicionais
- múltiplas métricas
- múltiplas chaves
- agregação global
- `SinglePartition`
- cardinalidade

### 07 — Joins

Tipos estudados:

- `inner`
- `left`
- `right`
- `full`
- `left_semi`
- `left_anti`

Aplicações de qualidade de dados:

```text
pedidos LEFT ANTI clientes
        ↓
pedidos com cliente inexistente
```

```text
pagamentos LEFT ANTI pedidos
           ↓
pagamentos órfãos
```

Também são estudadas relações:

```text
1:1
1:N
N:N
```

e o risco de multiplicação de linhas e métricas infladas.

#### BroadcastHashJoin

```text
Tabela pequena
      ↓
BroadcastExchange
      ↓
Executores
      ↓
match local com a tabela maior
```

#### SortMergeJoin

```text
Tabela A                  Tabela B
   ↓                         ↓
Exchange                  Exchange
   ↓                         ↓
Shuffle                   Shuffle
   ↓                         ↓
Sort                      Sort
      \                   /
       \                 /
        SortMergeJoin
```

---

## 🔥 Narrow x Wide

### Narrow

Cada partição consegue executar sua transformação sem depender de dados de outras partições.

Exemplos:

```text
filter
select
withColumn
```

### Wide

A operação exige redistribuição dos dados entre partições.

Exemplos comuns:

```text
groupBy
repartition
orderBy
SortMergeJoin
```

```text
Narrow → processamento independente entre partições

Wide → redistribuição / dependência entre partições
       ↓
     Shuffle
```

---

## 🔄 Shuffle, Exchange e agregações

```text
HashAggregate parcial
        ↓
calcula resultados locais
        ↓
Exchange
        ↓
indica uma nova distribuição
        ↓
Shuffle
        ↓
redistribui os dados
        ↓
HashAggregate final
        ↓
resultado definitivo
```

Importante:

```text
Exchange ≠ cálculo

Shuffle ≠ agregação

Shuffle = redistribuição de dados entre partições
```

---

## 📁 Estrutura do projeto

```text
.
├── datasets/
│   ├── 01_fundamentos/
│   ├── 02_lineage_DAG/
│   ├── 03_transformacoes/
│   ├── 04_filtros_ordenacao/
│   ├── 05_particoes/
│   ├── 06_agregacoes/
│   └── 07_joins/
│
├── desafios/
│   ├── desafio_01_fundamentos.ipynb
│   ├── desafio_02_lineage_DAG.ipynb
│   ├── desafio_03_transformacoes.ipynb
│   ├── desafio_04_filtros_ordenacao.ipynb
│   ├── desafio_05_particoes.ipynb
│   ├── desafio_06_agregacoes.ipynb
│   └── desafio_07_joins.ipynb
│
├── docs/
├── notebooks/
├── outputs/
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── README.md
└── requirements.txt
```

---

## 📦 Estratégia para datasets

Datasets pequenos são usados para validar lógica e visualizar facilmente os resultados.

```text
dataset pequeno
→ entender lógica
```

Datasets maiores são usados quando o objetivo é observar comportamento real de Spark em escala:

```text
dataset grande
→ partições
→ Shuffle
→ joins
→ performance
```

Os CSVs grandes utilizados nos testes de escala **não são versionados no GitHub** e estão incluídos no `.gitignore`.

---

## 🐳 Ambiente

- Python
- PySpark
- Apache Spark 3.5.x
- Jupyter Notebook
- Docker
- Docker Compose

O ambiente em containers ajuda a manter a execução reproduzível e isolada das configurações locais.

---

## ▶️ Executando o projeto

Clone o repositório:

```bash
git clone <URL_DO_REPOSITORIO>
cd <NOME_DO_REPOSITORIO>
```

Suba o ambiente:

```bash
docker compose up --build
```

Depois utilize os notebooks em:

```text
notebooks/
```

e os desafios em:

```text
desafios/
```

---

## 🗺️ Roadmap

```text
Fundamentos
  ✅

Lineage / DAG
  ✅

Transformações
  ✅

Filtros / Ordenação
  ✅

Partições
  ✅

Agregações
  ✅

Joins
  ✅

Parquet
  ↓
JDBC
  ↓
Performance
  ↓
Pipeline final
```

O pipeline final deverá aproximar o estudo de um cenário real de Engenharia de Dados:

```text
CSV / JSON / Banco / API
          ↓
        Spark
          ↓
leitura e processamento
schema e tipos
tratamento técnico
joins
particionamento
          ↓
   Parquet / Data Warehouse
          ↓
         dbt
          ↓
transformações analíticas
modelagem
testes
marts
```

---

## 💡 Filosofia do projeto

Spark não está sendo estudado aqui apenas como uma biblioteca de funções.

O objetivo é desenvolver a capacidade de perguntar:

```text
Essa transformação é Narrow ou Wide?

Vai ocorrer Shuffle?

Como os dados estão particionados?

Qual estratégia de Join o Spark escolheu?

Existe risco de multiplicação de linhas?

O Physical Plan faz sentido para o volume de dados?
```

Essas perguntas fazem parte do uso consciente de Spark em pipelines de Engenharia de Dados.

---

## 🛠️ Tecnologias

`Python` · `PySpark` · `Apache Spark` · `Docker` · `Jupyter` · `CSV` · `Parquet` · `JDBC`

---

## 📌 Status

Repositório em evolução.

Os módulos são adicionados conforme o avanço dos estudos, sempre acompanhados por exercícios, desafios e análise do comportamento interno do Spark.

---

## 👨‍💻 Objetivo profissional

Este repositório faz parte de uma trilha de desenvolvimento em **Engenharia de Dados**, com foco na construção de pipelines modernos e no processamento eficiente de grandes volumes de dados.

A proposta é utilizar Spark principalmente como motor de processamento e integração de dados dentro de arquiteturas modernas, combinando-o posteriormente com tecnologias como Data Warehouses, Lakehouses e dbt.
