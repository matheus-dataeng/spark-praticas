# Caderno de estudos — Apache Spark e PySpark

**Curso concluído em 1º de novembro de 2026**, com dez aulas, dez desafios individuais e um Desafio Final Integrador.

Este caderno reúne os conceitos e os experimentos do curso, dos primeiros DataFrames à análise de performance. Os capítulos acompanham o código dos notebooks e retomam as dúvidas dos desafios, com foco em entender como o Spark processa os dados e quais custos entram nas decisões de Engenharia de Dados.

## Aulas e desafios

| Aula e capítulo | Assuntos principais | Notebook | Desafio |
| --- | --- | --- | --- |
| [01 — Fundamentos](01_fundamentos.md) | SparkSession, CSV, schema, DataFrame, Column, actions e Lazy Evaluation | [Aula 01](../notebooks/aula_01_fundamentos.ipynb) | [Desafio 01](../desafios/desafio_01_fundamentos.ipynb) |
| [02 — Lineage, DAG e Catalyst](02_lineage_dag.md) | Dependências entre operações, otimização e leitura dos planos | [Aula 02](../notebooks/aula_02_lineage_dag.ipynb) | [Desafio 02](../desafios/desafio_02_lineage_DAG.ipynb) |
| [03 — Transformações](03_transformacoes.md) | Seleção, cálculo e renomeação de colunas; projeções e Column Pruning | [Aula 03](../notebooks/aula_03_transformacoes.ipynb) | [Desafio 03](../desafios/desafio_03_transformacoes.ipynb) |
| [04 — Filtros e ordenação](04_filtros_ordenacao.md) | Condições booleanas, filtros Narrow, ordenação e `limit` | [Aula 04](../notebooks/aula_04_filtros_ordenacao.ipynb) | [Desafio 04](../desafios/desafio_04_filtros_ordenacao.ipynb) |
| [05 — Partições](05_particoes.md) | Tasks, Narrow/Wide, `repartition`, `coalesce`, `Exchange` e Shuffle | [Aula 05](../notebooks/aula_05_particoes.ipynb) | [Desafio 05](../desafios/desafio_05_particoes.ipynb) |
| [06 — Agregações](06_agregacoes.md) | Agrupamentos, métricas condicionais, `countDistinct` e agregação parcial/final | [Aula 06](../notebooks/aula_06_agregacoes.ipynb) | [Desafio 06](../desafios/desafio_06_agregacoes.ipynb) |
| [07 — Joins](07_joins.md) | Cardinalidade, integridade, `left_anti`, `left_semi`, chaves compostas e estratégias físicas | [Aula 07](../notebooks/aula_07_joins.ipynb) | [Desafio 07](../desafios/desafio_07_joins.ipynb) |
| [08 — Parquet](08_parquet.md) | Escrita e releitura, Snappy, pruning, pushdown e Small Files | [Aula 08](../notebooks/aula_08_parquet.ipynb) | [Desafio 08](../desafios/desafio_08_parquet.ipynb) |
| [09 — JDBC e PostgreSQL](09_jdbc_postgresql.md) | Leitura paralela, bounds, filtros na origem, `fetchsize`, `batchsize` e modos de escrita | [Aula 09](../notebooks/aula_09_jdbc.ipynb) | [Desafio 09](../desafios/desafio_09_jdbc.ipynb) |
| [10 — Performance](10_performance.md) | Planos físicos, partições, Data Skew, joins, AQE, cache/persistência e Spark UI | [Aula 10](../notebooks/aula_10_performance.ipynb) | [Desafio 10](../desafios/desafio_10_performance.ipynb) |

## Como usar o caderno

Leia cada capítulo junto com o notebook da aula e consulte o desafio para acompanhar a aplicação dos conceitos. As saídas ajudam a relacionar as transformações aos resultados; as respostas pessoais registram o raciocínio desenvolvido durante os exercícios.

Na leitura dos planos, vale separar a estratégia escolhida de seu custo: `explain(True)` mostra os operadores, enquanto o Spark UI permite investigar Jobs, Stages, Tasks, Shuffle e Spill. Essa distinção orientou os estudos de performance no fim do curso.

## Aula 10: o que os testes mostraram

A base sintética tinha **2 milhões de registros em oito partições**, com 90% das linhas no grupo zero. Ao usar `repartition(8, "grupo")`, uma partição recebeu **1.844.445 registros**. Sem a chave, `repartition(8)` distribuiu **250.000 por partição**. O número de partições permaneceu igual; a regra de distribuição mudou o equilíbrio. Uma operação posterior por chave ainda poderia concentrar novamente os dados.

Também foram comparados `repartition(4)` e `coalesce(4)` sobre a base uniforme. Ambos chegaram a 500 mil registros por partição, mas pelos caminhos distintos mostrados nos planos: redistribuição com `Exchange` no primeiro e redução com `Coalesce` no segundo.

Nos joins com a dimensão de dez grupos, BroadcastHashJoin e SortMergeJoin produziram os mesmos dois milhões de registros e as mesmas contagens por descrição. O broadcast evitou redistribuir a base principal por chave para o join; o SortMerge apresentou `Exchange` e `Sort` nos dois lados.

O AQE estava habilitado, com `AdaptiveSparkPlan` nos planos. Os testes de cache incluíram materialização por `count()`, reutilização com `InMemoryTableScan` e liberação com `unpersist()`. Cache pode evitar recomputações, mas não elimina os Shuffles exigidos pelas consultas seguintes; `persist()` permite escolher o nível de armazenamento.

## Desafio Final Integrador

O [Desafio Final Integrador](../desafios/desafio_11/desafio_11_final.ipynb) concluiu o curso com um pipeline de marketplace fictício e dados sintéticos reproduzíveis. As cinco partes do [enunciado](../desafios/desafio_11/ENUNCIADO_DESAFIO_FINAL.md) abrangem ingestão e diagnóstico; qualidade, joins e agregações; Parquet; PostgreSQL/JDBC; e performance com Spark UI.

Os CSVs usados no exercício estão no [pacote de dados do integrador](../datasets/desafio_final_integrador/dados_desafio_final.zip).

O trabalho reuniu indicadores por mês, UF e categoria, armazenamento dos pedidos tratados e validações da persistência JDBC. A tabela de indicadores apresentou **54 registros** tanto no Spark quanto na leitura de retorno do PostgreSQL. O notebook detalha as verificações e os limites dos testes; o [índice dos desafios](../desafios/README.md) apresenta as entregas do curso.

## Material complementar

- [Glossário técnico](glossario.md): termos e distinções usados nos estudos.
- [Mapa da arquitetura e trajetória](arquitetura_do_curso.md): relação entre os módulos e as etapas de processamento.
- [Índice de estudos anterior](indice_de_estudos.md): organização anterior do caderno, mantida como registro.
- [README do projeto](../README.md): apresentação do curso e instruções do ambiente.

Algumas notas dos capítulos refletem revisões anteriores. O Desafio 04, por exemplo, está preenchido e seu plano já usa `idade >= 35`; referências à cópia vazia ou ao filtro antigo são históricas. As etapas descritas como futuras no mapa de arquitetura também foram concluídas. Os caminhos `/workspace/...` dos exemplos correspondem ao laboratório Docker.
