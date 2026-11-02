# Desafio Final Integrador — Apache Spark para Engenharia de Dados

Chegamos à última avaliação do nosso curso! Este desafio vai reunir os conhecimentos das Aulas 01 a 10, simulando uma situação de Engenharia de Dados em que você precisará tomar suas próprias decisões técnicas.

Diferentemente dos exercícios anteriores, não vou indicar exatamente quais funções utilizar em cada operação. Quero avaliar sua capacidade de escolher a abordagem, desenvolver o código, interpretar os planos físicos e justificar suas decisões.

Preparei também um conjunto de dados sintéticos e reproduzíveis, com volume suficiente para praticar Spark.

## Pacote do Desafio Final

Enunciado completo, dados de entrada e instruções de execução.

Os três CSVs sintéticos estão prontos no arquivo [dados_desafio_final.zip](dados_desafio_final.zip). O pacote contém apenas os dados de entrada, sem a solução do desafio.

| Formato | Entrega | Nível |
| --- | --- | --- |
| 5 partes | 1 notebook | Intermediário |

## Cenário — Marketplace e Inteligência de Pedidos

Você foi encarregado de construir um fluxo de processamento de dados de um marketplace fictício.

A empresa recebe milhões de pedidos e possui cadastros de clientes e produtos. Existem problemas de qualidade, registros sem correspondência nas dimensões e uma concentração excessiva de pedidos em determinado produto.

Sua responsabilidade será ingerir, validar, integrar, processar e disponibilizar indicadores.

## Dados fornecidos

| Fonte | Quantidade |
| --- | ---: |
| Pedidos | 2 milhões |
| Clientes | 20 mil |
| Produtos | 300 |

Problemas intencionais nos dados:

- IDs de clientes e produtos inexistentes nas dimensões.
- Pedidos com quantidades inválidas.
- Uma chave de produto concentrando grande parte dos registros.
- Diferentes status, canais e períodos de pedidos.

Os dados são sintéticos, mas representam situações encontradas em pipelines reais.

## Preparação do ambiente

O arquivo `datasets/desafio_final_integrador/dados_desafio_final.zip` acompanha o repositório. Para extrair os dados, execute no terminal do container, a partir da raiz do repositório:

```bash
python -m zipfile -e datasets/desafio_final_integrador/dados_desafio_final.zip datasets/desafio_final_integrador
```

Após a extração, a estrutura será:

```text
datasets/
└── desafio_final_integrador/
    ├── ENUNCIADO.md
    ├── LEIA_PRIMEIRO.txt
    ├── dados_desafio_final.zip
    └── dados/
        ├── clientes.csv
        ├── pedidos.csv
        └── produtos.csv
```

Os três CSVs estão prontos para leitura no Spark. São dados sintéticos, com as quantidades e os problemas intencionais descritos acima.

O ZIP é versionado; a pasta `datasets/desafio_final_integrador/dados/` permanece ignorada pelo Git. Não adicione os CSVs extraídos ao repositório.

O notebook de entrega será:

```text
desafios/desafio_11/desafio_11_final.ipynb
```

## Parte A — Ingestão e diagnóstico

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

| Indicador | Descrição |
| --- | --- |
| Quantidade de pedidos | Total de pedidos concluídos |
| Unidades vendidas | Soma das quantidades |
| Receita líquida | Valor das vendas após desconto |

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
4. Comparar os planos físicos de `BroadcastHashJoin` e `SortMergeJoin` usando a dimensão de produtos.
5. Verificar se o AQE está habilitado e identificar `AdaptiveSparkPlan`.
6. Demonstrar reutilização de dados com `cache()`, identificando `InMemoryTableScan` e finalizando com `unpersist()`.
7. Investigar Jobs, Stages, Tasks, Shuffle Read/Write e Spill no Spark UI.

Para comparar o desempenho das estratégias de join, utilize a mesma amostra de até 200 mil pedidos nos dois experimentos, evitando sobrecarregar seu ambiente Docker.

**Pergunta E:** qual gargalo você conseguiu comprovar com as métricas? Qual problema é apenas uma hipótese? Que otimizações você realmente justificaria?
