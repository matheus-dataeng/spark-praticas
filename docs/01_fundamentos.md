# Aula 01 — Fundamentos do Apache Spark

> **Fontes:** [`aula_01_fundamentos.ipynb`](../notebooks/aula_01_fundamentos.ipynb) e [`desafio_01_fundamentos,.ipynb`](../desafios/desafio_01_fundamentos,.ipynb). Os exemplos abaixo reproduzem ou adaptam trechos existentes nesses arquivos.

## 1. SparkSession e primeiro DataFrame

O ponto de entrada do notebook é uma `SparkSession`. Ela representa o contexto pelo qual acessamos a API Spark SQL, configuramos a aplicação e construímos DataFrames:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("aula_01_fundamentos").getOrCreate()

df = spark.read.csv(
    "/workspace/datasets/01_fundamentos/clientes.csv",
    header=True,
    inferSchema=True,
)
```

O exemplo usa `clientes.csv`, com campos como `cliente_id`, `nome`, `idade`, `cidade`, `uf`, `renda_mensal`, `ativo` e `score`. `header=True` interpreta a primeira linha como cabeçalho. `inferSchema=True` faz uma análise dos valores para descobrir tipos como inteiro, double e booleano, em vez de tratar tudo como texto. **Inferência é conveniente para estudo, mas em pipelines de produção um schema explícito pode oferecer mais previsibilidade.**

## 2. DataFrame, Column e schema

Um **DataFrame** é uma representação distribuída de dados estruturados associada a um plano de consulta. Uma **Column** é uma expressão que identifica ou calcula uma coluna dentro desse plano; não é uma lista Python de valores.

No notebook, foram comparados:

```python
print(df)           # Exibe uma representação do DataFrame e seus tipos
print(type(df))     # pyspark.sql.dataframe.DataFrame
print(type(df.idade)) # pyspark.sql.column.Column
```

Também foi chamado `df.explain()`, que mostrou o operador de leitura `FileScan csv`, o caminho do arquivo e as colunas presentes no `ReadSchema`. Esse comando descreve o plano: **não equivale a materializar todas as linhas para apresentação**.

## 3. Transformations, actions e Lazy Evaluation

Transformações como `select()`, `filter()` e `withColumn()` **descrevem** um novo DataFrame, mas não executam, por si só, a leitura e o processamento completos. O Spark consegue examinar o conjunto dessas operações antes da execução. Uma *action*, como `show()` ou `count()`, solicita um resultado concreto e dispara a avaliação do plano necessário.

```text
read.csv → select → withColumn → filter
             transformações lazy
                    ↓
                  show()
                    ↓
             execução no Spark
```

O notebook chamou `df.show()` e observou os primeiros registros de clientes. Diferentemente de `print(df)`, `show()` exige efetivamente dados para produzir a saída.

## 4. Imutabilidade

Operações da DataFrame API não alteram o objeto original em memória como uma edição in-place de uma planilha. Uma transformação devolve **outro DataFrame com outro plano**:

```python
df_novo = df.select("cliente_id", "nome", "idade")
```

É possível reatribuir `df`, mas nomes distintos ajudam na rastreabilidade durante o aprendizado e na depuração.

## O que foi feito no notebook da aula

O notebook criou e encerrou uma `SparkSession`, leu `clientes.csv` com inferência de schema, executou `show()`, inspecionou `explain()`, imprimiu a representação e os tipos de `DataFrame`/`Column`. A seção final registra a diferença entre transformações lazy e actions.

## O que foi feito no desafio 01

O desafio retomou a leitura do mesmo CSV e construiu um fluxo próprio:

```python
df_novo = (
    df.select("cliente_id", "nome", "idade", "cidade", "renda_mensal", "ativo")
      .withColumn("renda_anual", F.col("renda_mensal") * 12)
      .withColumn(
          "faixa_etaria",
          F.when((F.col("idade") >= 18) & (F.col("idade") < 60), "Adulto")
           .when(F.col("idade") >= 60, "Idoso")
           .otherwise("Menor de idade"),
      )
)

df_filtrado = df_novo.filter(F.col("ativo") & (F.col("idade") >= 30))
```

Foram calculadas `renda_anual` e `faixa_etaria`, selecionados clientes ativos com pelo menos 30 anos e mostradas linhas com `show()`. As respostas em Markdown explicaram a diferença entre `print(df)`, `df.show()` e `df.explain()`, além de identificar a execução lazy. **Observação de precisão:** não foi somente o último `show()` que foi action; houve chamadas anteriores a `df.show()` no próprio desafio. E `explain()` inspeciona o plano, não é um relatório de tudo que já foi executado.

## Para revisar

- `DataFrame` descreve dados **e** plano; `Column` representa uma expressão.
- `inferSchema` é inferência, não garantia de tipos perfeitos.
- `show()`/`count()` solicitam cálculo; `select()`/`filter()` constroem o plano.
- DataFrames são imutáveis no sentido da API de transformações.

**Próximo módulo:** [02 — Lineage, DAG e Catalyst](02_lineage_dag.md).
