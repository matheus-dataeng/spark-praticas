# Caderno de estudo — Apache Spark e PySpark

Esta pasta contém os **materiais técnicos das Aulas 01 a 09**, reescritos a partir dos **notebooks de aula e dos desafios efetivamente enviados**. Diferentemente de um roteiro de objetivos, cada capítulo explica os conceitos, mostra trechos de código utilizados, comenta o que o notebook fez e discute as respostas/resultados dos desafios.

## Índice

| Aula | Assunto | Principal evidência prática |
|---|---|---|
| [01](01_fundamentos.md) | DataFrames, Column, lazy e actions | CSV de clientes; `show`, `print`, `explain`; renda anual e faixa etária |
| [02](02_lineage_dag.md) | Lineage, DAG e Catalyst | Filtros separados x combinados; os quatro planos de `explain(True)` |
| [03](03_transformacoes.md) | `select`, `drop`, `withColumn`, `alias` | Tratamento de clientes; projeções e Column Pruning |
| [04](04_filtros_ordenacao.md) | Booleanos, ordenação, `limit` | `Filter`, `Sort`, `Exchange`, `CollectLimit` |
| [05](05_particoes.md) | Spark Partitions, Narrow/Wide, Shuffle | `repartition` e `coalesce`; contagem de partições |
| [06](06_agregacoes.md) | Agregações e etapas parciais/finais | `HashAggregate`, `Exchange`, `SinglePartition`, `countDistinct` |
| [07](07_joins.md) | Cardinalidade, integridade, joins físicos | `left_anti`, joins encadeados, 4.834 linhas, Broadcast x SortMerge |
| [08](08_parquet.md) | Armazenamento colunar e pruning | Conversão CSV→Parquet, Snappy, `ReadSchema`, `PartitionFilters` |
| [09](09_jdbc_postgresql.md) | JDBC e PostgreSQL em Docker | 2 milhões de linhas, 1 x 4 partições, bounds, fetchsize/batchsize |

### Documentos complementares

- [Mapa da arquitetura e trajetória](arquitetura_do_curso.md)
- [Glossário técnico](glossario.md)

## Como estudar por este material

Leia o capítulo junto com `notebooks/aula_XX_*.ipynb`, reproduza mentalmente o plano físico e, em seguida, consulte `desafios/desafio_XX_*.ipynb`. As seções **“O que foi feito no notebook”** e **“O que foi feito no desafio”** foram produzidas com base nos arquivos, não em modelos genéricos de exercícios.

**Observação sobre a Aula 04:** a cópia de `desafio_04_filtros_ordenacao.ipynb` recebida contém zero células. O capítulo explica o conteúdo do notebook da aula e deixa essa limitação transparente.

**Observação sobre a Aula 09:** também foi analisado o arquivo `notebooks/CREATE TABLE public.sql`, que contém a geração de 2 milhões de registros por `generate_series`, os testes de contagem e DDLs das tabelas de destino.

## Convenções

Os caminhos de arquivos dos exemplos são os utilizados no ambiente Docker do curso (`/workspace/...`). Nomes de colunas e DataFrames são preservados quando ajudam a acompanhar os notebooks. Exemplos com reticências ou esquemas desenhados são **ilustrações didáticas**, e não transcrições literais de `explain(True)`.

Os números mostrados (por exemplo 4.834 ou 2.000.000) pertencem aos **datasets específicos da trilha**, não são garantias para outros ambientes ou versões do Spark.

> A documentação se concentra em conceitos e mecanismos. Alterações de estrutura, thresholds e estratégias podem variar com Spark, AQE, estatísticas e configuração do cluster.
