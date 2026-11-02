# Caderno de estudo — Apache Spark e PySpark

Esta pasta contém os **materiais técnicos das Aulas 01 a 10**, elaborados a partir dos **notebooks de aula e dos desafios efetivamente enviados**. Diferentemente de um roteiro de objetivos, cada capítulo explica os conceitos, mostra trechos de código utilizados, comenta o que o notebook fez e discute as respostas e os resultados dos desafios.

## Índice

| Aula | Assunto | Principal evidência prática |
|---|---|---|
| [01](01_fundamentos.md) | DataFrames, Column, Lazy Evaluation e actions | CSV de clientes; `show`, `print`, `explain`; renda anual e faixa etária |
| [02](02_lineage_dag.md) | Lineage, DAG e Catalyst | Filtros separados × combinados; os quatro planos de `explain(True)` |
| [03](03_transformacoes.md) | `select`, `drop`, `withColumn`, `alias` | Tratamento de clientes; projeções e Column Pruning |
| [04](04_filtros_ordenacao.md) | Expressões booleanas, ordenação e `limit` | `Filter`, `Sort`, `Exchange`, `CollectLimit` |
| [05](05_particoes.md) | Spark Partitions, Narrow/Wide e Shuffle | `repartition` e `coalesce`; contagem e distribuição entre partições |
| [06](06_agregacoes.md) | Agregações e etapas parciais/finais | `HashAggregate`, `Exchange`, `SinglePartition`, `countDistinct` |
| [07](07_joins.md) | Cardinalidade, integridade e estratégias de join | `left_anti`, joins encadeados, 4.834 linhas, Broadcast × SortMerge |
| [08](08_parquet.md) | Armazenamento colunar e pruning | Conversão CSV → Parquet, Snappy, `ReadSchema`, `PartitionFilters` |
| [09](09_jdbc_postgresql.md) | JDBC e PostgreSQL em Docker | 2 milhões de linhas, 1 × 4 partições, bounds, `fetchsize`/`batchsize` |
| [10](10_performance.md) | Diagnóstico e otimização de performance | Data Skew, `repartition` × `coalesce`, BroadcastHashJoin × SortMergeJoin, AQE, cache e Spark UI |

### Documentos complementares

- [Mapa da arquitetura e trajetória](arquitetura_do_curso.md)
- [Glossário técnico](glossario.md)

## Como estudar por este material

Leia cada capítulo junto com `notebooks/aula_XX_*.ipynb`, acompanhe o raciocínio por trás das transformações e interprete o plano físico. Em seguida, consulte `desafios/desafio_XX_*.ipynb` e compare os requisitos do desafio com as implementações e análises realizadas.

As seções **“O que foi feito no notebook”** e **“O que foi feito no desafio”** foram produzidas com base nos materiais da trilha, não em modelos genéricos de exercícios.

**Observação sobre a Aula 04:** a cópia de `desafio_04_filtros_ordenacao.ipynb` utilizada na elaboração daquele capítulo continha zero células. O documento descreve o notebook da aula e informa essa limitação da versão analisada; ela não implica necessariamente que versões posteriores do desafio estejam vazias.

**Observação sobre a Aula 09:** também foi analisado o arquivo `notebooks/CREATE TABLE public.sql`, que contém a geração de 2 milhões de registros por `generate_series`, testes de contagem e DDLs das tabelas de destino.

**Observação sobre a Aula 10:** os experimentos utilizaram uma base sintética de 2 milhões de registros, incluindo uma distribuição intencionalmente desbalanceada. A comparação de partições identificou uma partição com **1.844.445 registros** no reparticionamento por chave. Foram examinados planos com BroadcastHashJoin e SortMergeJoin, o uso de cache (`InMemoryTableScan`), o AQE e métricas no Spark UI. A preferência por BroadcastHashJoin no cenário estudado foi tecnicamente justificada, mas **não foi estabelecido um ganho percentual de desempenho por meio de medições completas e equivalentes**.

## Convenções

Os caminhos dos exemplos correspondem aos utilizados no ambiente Docker do curso (`/workspace/...`). Nomes de colunas e DataFrames são preservados quando ajudam a acompanhar os notebooks. Exemplos com reticências ou diagramas são **ilustrações didáticas**, e não transcrições literais de `explain(True)`.

Os números apresentados (por exemplo, **4.834**, **1.844.445** ou **2.000.000**) pertencem aos **datasets e experimentos específicos da trilha** e não são garantias para outros ambientes ou versões do Spark.

> A documentação se concentra em conceitos e mecanismos. Operadores, estratégias de execução, quantidade de partições e resultados de performance podem variar conforme a versão do Spark, o AQE, as estatísticas disponíveis, os dados e a configuração do ambiente. O plano físico ajuda a entender a execução planejada; métricas do Spark UI permitem investigar seus custos efetivos.
