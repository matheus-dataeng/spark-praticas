# Glossário técnico — Spark e PySpark

| Termo | Significado no contexto dos notebooks |
|---|---|
| **Action** | Operação que solicita materialização/resultado, como `show()` ou `count()`; escrita também executa o plano necessário. |
| **AdaptiveSparkPlan / AQE** | Execução adaptativa capaz de ajustar escolhas físicas com informações obtidas durante a execução. |
| **Batchsize** | Tamanho do lote de registros enviados por uma conexão de **escrita JDBC**. Não altera Spark Partitions. |
| **BroadcastExchange** | Prepara/disponibiliza o lado pequeno para o broadcast em um join. |
| **BroadcastHashJoin** | Estratégia de join com hash do lado transmitido; `BuildLeft/BuildRight` indica lado da hash do join atual. |
| **Catalyst** | Otimizador/compilador de planos SQL do Spark. |
| **Column** | Expressão de coluna PySpark, distinta de uma lista de valores Python. |
| **Column Pruning** | Reduz as colunas que precisam ser lidas/processadas. Evidência: `ReadSchema`. |
| **Data Skipping** | Ignora blocos internos que não podem satisfazer um filtro, usando metadados/estatísticas. |
| **DAG** | Grafo acíclico dirigido que representa dependências de operações/estágios. |
| **Data Skew** | Desequilíbrio no volume de linhas ou custo de trabalho entre partições/Tasks. |
| **DataFilters** | Condições sobre dados exibidas na leitura do Physical Plan. |
| **Executor** | Processo que executa Tasks e gerencia recursos de execução da aplicação Spark. |
| **Exchange** | Operador físico que estabelece uma distribuição necessária; frequentemente envolve Shuffle. |
| **Fetchsize** | Quantidade de linhas buscadas por lote pelo driver na **leitura JDBC**. |
| **FileScan** | Operador de leitura física de arquivos como CSV e Parquet. |
| **HashAggregate partial** | Produz estados parciais de agregação por partição, reduzindo dados que podem atravessar Shuffle. |
| **HashAggregate final** | Combina estados parciais após a redistribuição e produz o resultado final. |
| **JDBC** | API Java de acesso a bancos relacionais; Spark usa o driver PostgreSQL para comunicação. |
| **Lazy Evaluation** | Plano é construído sem processar inteiramente os dados até que uma ação exija um resultado. |
| **Left anti** | Retorna linhas da esquerda sem correspondência à direita. |
| **Left semi** | Retorna linhas da esquerda que possuem correspondência; não acrescenta colunas do lado direito. |
| **Lineage** | Dependências entre transformações que geraram um DataFrame. |
| **LowerBound** | Referência inferior para calcular faixas JDBC; **não é filtro**. |
| **Narrow Transformation** | Pode processar uma partição sem redistribuição de dados entre partições. |
| **NumPartitions (JDBC)** | Quantidade de faixas de leitura, relacionada ao número de conexões JDBC simultâneas possíveis. |
| **Parquet** | Formato de armazenamento colunar e tipado, com metadados e compressão. |
| **Partition Pruning** | Evita ler partições físicas do dataset incompatíveis com o filtro. Evidência: `PartitionFilters`. |
| **PartitionBy (write)** | Organiza a saída em diretórios/arquivos por valores de coluna. Diferente de `repartition`. |
| **Predicate Pushdown** | Envia filtros para perto da fonte/leitor. Evidência: `PushedFilters` quando suportado. |
| **Repartition** | Redistribui Spark Partitions, tipicamente criando Shuffle. |
| **Coalesce** | Normalmente reduz Spark Partitions sem novo Shuffle completo quando possível. |
| **Row Group** | Bloco interno de arquivo Parquet, com chunks colunares e possíveis estatísticas. |
| **Shuffle** | Movimentação/redistribuição dos dados entre partições. Não é sinônimo de agregação. |
| **SinglePartition** | Distribuição física usada em certas consolidações globais. |
| **Small Files** | Muitos arquivos pequenos que aumentam metadados, planejamento e overhead de I/O. |
| **SortMergeJoin** | Join que normalmente redistribui dois lados pela chave, ordena e faz o merge. |
| **Spark Partition** | Unidade lógica de dados/parallelismo da execução Spark. |
| **Task** | Unidade de trabalho executada sobre uma partição. |
| **UpperBound** | Referência superior para calcular faixas JDBC; **não restringe automaticamente linhas**. |
| **Wide Transformation** | Requer redistribuição/dependências entre partições, frequentemente envolvendo Shuffle. |

## Quatro perguntas que resolvem muitas confusões

```text
O que divide o PROCESSAMENTO?           Spark Partition
O que divide DIRETÓRIOS no Data Lake?    write.partitionBy(...)
O que divide BLOCOS dentro do Parquet?   Row Group
O que divide INTERVALOS de leitura JDBC? partitionColumn + bounds + numPartitions
```

```text
COUNT(*)              → quantidade de registros
MIN/MAX(partitionColumn) → intervalo de valores
ReadSchema            → colunas de leitura
PushedFilters         → filtros encaminhados ao leitor
PartitionFilters      → partições físicas elegíveis
```
