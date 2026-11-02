# Parte A — Ingestão e diagnóstico

**Objetivo:** construir a etapa inicial de ingestão e compreender a estrutura dos dados.

Você deverá:

1. Ler as três fontes CSV com schemas explícitos.
2. Validar quantidades, tipos de dados e colunas.
3. Verificar a quantidade de Spark Partitions da tabela de pedidos.
4. Criar uma coluna `ano_mes` a partir da data do pedido.
5. Investigar a distribuição de registros por status e identificar os cinco produtos mais frequentes.
6. Inspecionar pelo menos um `explain(True)`.

**Conceitos avaliados:** Lazy Evaluation, Catalyst, DataFrames, schemas, Column Pruning, Narrow e Wide Transformations.

**Pergunta A:** quais vantagens um schema explícito oferece na leitura de arquivos CSV? Quais das operações realizadas exigiram Shuffle e por quê?

## Parte B — Qualidade, joins e agregações

**Objetivo:** investigar inconsistências e construir indicadores confiáveis.

Você deverá:

1. Identificar pedidos sem clientes correspondentes.
2. Identificar pedidos sem produtos correspondentes.
3. Detectar quantidades inválidas e estabelecer uma regra de tratamento.
4. Verificar a unicidade das chaves das dimensões, evitando multiplicações indevidas.
5. Construir um DataFrame de pedidos válidos, integrando informações de clientes e produtos.
6. Produzir indicadores de pedidos concluídos por `ano_mes`, `uf` e `categoria`.

Seus indicadores devem conter:

| Indicador             | Descrição                      |
| --------------------- | ------------------------------ |
| Quantidade de pedidos | Total de pedidos concluídos    |
| Unidades vendidas     | Soma das quantidades           |
| Receita líquida       | Valor das vendas após desconto |

Utilize os planos físicos para examinar estratégias de join e agregação, observando operadores como `HashAggregate partial`, `Exchange` e `HashAggregate final`.

**Pergunta B:** como a cardinalidade de um join pode comprometer os indicadores? Quando `left_anti` é mais apropriado que `inner`? Qual é o papel do `left_semi` nessa análise?

## Parte C — Armazenamento em Parquet

**Objetivo:** persistir os dados tratados utilizando armazenamento colunar e explorar otimizações de leitura.

Você deverá:

1. Escrever os pedidos curados em Parquet.
2. Escolher uma estratégia de particionamento físico apropriada, considerando `ano_mes`.
3. Ler novamente o diretório Parquet.
4. Filtrar um mês específico e selecionar somente algumas colunas.
5. Inspecionar o plano físico, procurando `PartitionFilters`, `ReadSchema` e `PushedFilters`, quando aplicáveis.
6. Analisar o número de arquivos produzidos e o risco de Small Files.

**Pergunta C:** por que particionar fisicamente por mês pode fazer sentido nesse dataset, enquanto particionar pelo identificador de cliente pode trazer problemas?

## Parte D — Integração PostgreSQL/JDBC

**Objetivo:** disponibilizar os indicadores processados para consumo em um banco de dados relacional.

Você deverá:

1. Persistir os indicadores agregados no PostgreSQL, utilizando JDBC.
2. Utilizar a tabela `public.desafio_final_indicadores`.
3. Validar quantidade de registros, colunas, tipos e uma amostra diretamente no PostgreSQL.
4. Ler novamente essa tabela utilizando Spark JDBC.
5. Verificar a consistência entre os indicadores antes e depois da persistência.
6. Justificar suas configurações de leitura e escrita.

Explique também os riscos de `append` e `overwrite`, e o papel de configurações como `numPartitions`, `fetchsize`, `batchsize`, `lowerBound` e `upperBound`.

**Pergunta D:** como você garantiria que os dados persistidos no PostgreSQL correspondem exatamente aos indicadores produzidos pelo Spark?

## Parte E — Performance e diagnóstico

**Objetivo:** aplicar os conhecimentos da Aula 10 em um pipeline mais próximo de uma situação real.

Você deverá:

1. Comparar `repartition(8)` com `repartition(8, "produto_id")` no DataFrame detalhado de pedidos.
2. Utilizar `spark_partition_id()` para verificar a distribuição real de registros.
3. Identificar o impacto de Data Skew.
4. Comparar os planos físicos de BroadcastHashJoin e SortMergeJoin usando a dimensão de produtos.
5. Verificar se o AQE está habilitado e identificar `AdaptiveSparkPlan`.
6. Demonstrar reutilização de dados com `cache()`, identificando `InMemoryTableScan` e finalizando com `unpersist()`.
7. Investigar Jobs, Stages, Tasks, Shuffle Read/Write e Spill no Spark UI.

Para comparar o desempenho das estratégias de join, utilize **a mesma amostra de até 200 mil pedidos** nos dois experimentos, evitando sobrecarregar seu ambiente Docker.

**Pergunta E:** qual gargalo você conseguiu comprovar com as métricas? Qual problema é apenas uma hipótese? Que otimizações você realmente justificaria?
