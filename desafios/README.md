# Desafios práticos de Apache Spark e PySpark

Esta pasta reúne os exercícios que desenvolvi ao longo do curso, com o código, os resultados e minhas respostas conceituais. A trilha foi encerrada em **1º de novembro de 2026**, com os dez desafios individuais e o Desafio Final Integrador concluídos.

A teoria está no [caderno de estudos](../docs/README.md). As instruções para executar o ambiente estão no [README principal](../README.md).

## Desafios individuais

Os exercícios começam com a leitura de dados e avançam até a análise de estratégias de execução e performance.

| Notebook | Assuntos trabalhados |
| --- | --- |
| [01 — Fundamentos](desafio_01_fundamentos.ipynb) | SparkSession, CSV, schema, DataFrames, transformações, actions e Lazy Evaluation. |
| [02 — Lineage e DAG](desafio_02_lineage_DAG.ipynb) | Dependências entre transformações, Catalyst e comparação de planos lógicos e físicos. |
| [03 — Transformações](desafio_03_transformacoes.ipynb) | Seleção de colunas, expressões, renomeações, aliases e Column Pruning. |
| [04 — Filtros e ordenação](desafio_04_filtros_ordenacao.ipynb) | Condições booleanas, transformações Narrow/Wide, ordenação, `limit` e planos físicos. |
| [05 — Partições](desafio_05_particoes.ipynb) | Paralelismo, `repartition`, `coalesce`, Shuffle e organização dos arquivos de saída. |
| [06 — Agregações](desafio_06_agregacoes.ipynb) | `groupBy`, agregações por chave e globais, `distinct`, `HashAggregate` e `Exchange`. |
| [07 — Joins](desafio_07_joins.ipynb) | Cardinalidade, integridade referencial, tipos de join, chaves compostas e multiplicação de linhas. |
| [08 — Parquet](desafio_08_parquet.ipynb) | Armazenamento colunar, Column Pruning, Predicate Pushdown, Partition Pruning e row groups. |
| [09 — JDBC e PostgreSQL](desafio_09_jdbc.ipynb) | Leitura e escrita JDBC, particionamento da leitura, filtros, `fetchsize` e validação de tipos. |
| [10 — Performance](desafio_10_performance.ipynb) | Data Skew, BroadcastHashJoin, SortMergeJoin, Cache, AQE e análise pelo Spark UI. |

## Desafio Final Integrador

O integrador encerrou o curso com um pipeline para um marketplace fictício. Os dados sintéticos são reproduzíveis e incluem referências sem cadastro, quantidades inválidas e concentração de pedidos em um produto. O trabalho reuniu o tratamento dessas situações, a construção de indicadores e a análise da execução.

- [Notebook final](desafio_11/desafio_11_final.ipynb)
- [Enunciado](desafio_11/ENUNCIADO_DESAFIO_FINAL.md)
- [Preparação e extração dos dados](../datasets/desafio_final_integrador/LEIA_PRIMEIRO.txt)

O desafio foi organizado em cinco partes:

1. **A — Ingestão e diagnóstico:** leitura dos CSVs com schemas explícitos, inspeção dos dados, criação de `ano_mes` e análise dos planos.
2. **B — Qualidade, joins e agregações:** auditoria de pedidos sem correspondência, tratamento de quantidades inválidas, verificação das chaves e cálculo dos indicadores por mês, UF e categoria.
3. **C — Parquet:** escrita dos pedidos tratados por `ano_mes` e leitura com filtro temporal e seleção de colunas para examinar Partition Pruning e Column Pruning.
4. **D — PostgreSQL/JDBC:** persistência dos indicadores e comparação dos dados recuperados com os resultados originais do Spark.
5. **E — Performance:** distribuição por partição, Data Skew, comparação de BroadcastHashJoin e SortMergeJoin sobre a mesma amostra, verificação do AQE e reutilização com Cache.

A ingestão inicial leu **2.000.000 de pedidos, 20.000 clientes e 300 produtos**. Após as validações e os joins, foram calculadas a quantidade de pedidos concluídos, as unidades vendidas e a receita líquida por mês, UF e categoria. Os **54 registros agregados** foram gravados em `public.desafio_final_indicadores` e recuperados via JDBC. As comparações salvas não encontraram chaves ausentes em nenhum dos sentidos nem divergências entre os indicadores comparados.

Os planos registram as estratégias de join, o uso de AQE e a leitura dos dados em Cache. A escolha de Broadcast foi fundamentada no tamanho reduzido da dimensão de produtos. Shuffle e distribuição desigual entre partições exigem investigação; sua presença, por si só, não comprova um gargalo. Cache evita recomputações dos dados reutilizados, mas as agregações ainda podem exigir Shuffle.

Os testes foram realizados no Docker local. O notebook final não preserva métricas completas e comparáveis nem capturas do Spark UI. Por isso, os registros permitem discutir os planos e os resultados, mas não sustentam uma comparação controlada de performance ou um ganho percentual.
