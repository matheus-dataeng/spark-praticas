# Apache Spark na Prática — Engenharia de Dados com PySpark

**Um laboratório de Engenharia de Dados para aprender Spark além da sintaxe: compreender planos de execução, distribuição de dados, integração de fontes e decisões de performance.**

`Apache Spark` · `PySpark` · `Python` · `SQL` · `PostgreSQL` · `Docker` · `Jupyter` · `Parquet` · `JDBC`

> **Propósito:** desenvolver a capacidade de construir e analisar pipelines de ingestão e processamento técnico com Spark, entendendo **por que** uma solução funciona, **como** ela é executada e **quando** uma decisão de arquitetura pode se tornar um gargalo.

Este repositório registra uma trilha **prática e progressiva** de estudos em Spark, com notebooks de aula, desafios individuais, datasets de diferentes tamanhos e documentação técnica. O uso de Spark é direcionado principalmente à **extração, integração e preparação técnica dos dados**; transformações analíticas podem ser realizadas posteriormente em SQL/dbt, conforme o desenho da arquitetura.

## O que diferencia este repositório?

Em vez de limitar cada aula a uma coleção de comandos PySpark, os exercícios são acompanhados por investigação do `explain(True)`, validação de resultados e comparação de escolhas de execução.

```text
Fonte de dados
     ↓
Spark DataFrame
     ↓
transformações + validações
     ↓
plano lógico → otimização → plano físico
     ↓
partições / tasks / shuffle / joins
     ↓
Parquet ou PostgreSQL
```

**A pergunta recorrente não é apenas “o código executou?”, mas “o que o Spark realmente fez?”**

## Evolução do curso

| Módulo | Conteúdo e prática | Situação |
| --- | --- | --- |
| [01 · Fundamentos](docs/01_fundamentos.md) | `SparkSession`, leitura CSV, schema, DataFrames, actions, transformations e lazy evaluation | Concluído |
| [02 · Lineage e DAG](docs/02_lineage_dag.md) | `explain(True)`, Catalyst e comparação entre planos lógicos equivalentes | Concluído |
| [03 · Transformações](docs/03_transformacoes.md) | `select`, `drop`, `withColumn`, aliases e Column Pruning | Concluído |
| [04 · Filtros e ordenação](docs/04_filtros_ordenacao.md) | Filtros compostos, negação, `orderBy`, `limit` e custo de ordenações globais | **Aula concluída; desafio pendente** |
| [05 · Partições](docs/05_particoes.md) | Tasks, Executors, `repartition`, `coalesce`, Narrow/Wide e Shuffle | Concluído |
| [06 · Agregações](docs/06_agregacoes.md) | `groupBy`, agregações condicionais, `countDistinct`, HashAggregate parcial/final | Concluído |
| [07 · Joins](docs/07_joins.md) | Cardinalidade, integridade referencial, semi/anti, BroadcastHashJoin e SortMergeJoin | Concluído |
| [08 · Parquet](docs/08_parquet.md) | Compressão, Row Groups, Column/Predicate/Partition Pruning e small files | Concluído |
| [09 · JDBC + PostgreSQL](docs/09_jdbc_postgresql.md) | Leitura/escrita JDBC, ingestão paralela, bounds, `fetchsize` e `batchsize` | Concluído |
| **10 · Performance** | Skew, cache/persist, Shuffle, análise de gargalos e boas decisões de execução | Planejado |
| **11 · Projeto final integrador** | Pipeline de ponta a ponta usando os conceitos do curso | Planejado |

A documentação aprofundada está no **[índice de estudos](docs/README.md)**. Cada arquivo em `docs/` explica o conteúdo e descreve o que foi feito no notebook e no desafio correspondente.

## Experimentos que já foram realizados

### 1. Planos físicos e agregações distribuídas

Nos módulos de partições e agregações foram comparados planos que envolvem redistribuição por chave:

```text
FileScan
    ↓
HashAggregate (partial)
    ↓
Exchange / Shuffle
    ↓
HashAggregate (final)
```

O `HashAggregate` parcial reduz os dados antes da redistribuição; o `Exchange` estabelece a distribuição necessária, enquanto o Shuffle movimenta dados entre partições. Os notebooks também exploram agrupamentos compostos, agregações globais com `SinglePartition` e o custo adicional de `countDistinct`.

### 2. Joins, qualidade de dados e cardinalidade

Foram utilizados conjuntos relacionados de **clientes, pedidos e pagamentos** para comparar joins e verificar integridade referencial:

```text
clientes ── INNER ── pedidos ── LEFT ── pagamentos
                  │                    │
                  └─ left_anti         └─ pagamentos órfãos
                     pedidos órfãos
```

Nos exercícios, `left_semi` identificou clientes com pedidos, e `left_anti` localizou registros sem correspondência. A integração válida entre clientes e pedidos produziu **4.834 linhas** no dataset pequeno. Também foram investigadas duplicidades, relações 1:N/N:N e o risco de métricas infladas.

Com os dados maiores, a análise passou a comparar **`BroadcastHashJoin` e `SortMergeJoin`**, incluindo `BuildLeft`, `BuildRight`, `BroadcastExchange`, `Sort` e `Exchange hashpartitioning`.

### 3. Parquet e leitura seletiva

O CSV foi convertido para **Parquet com compressão Snappy**. A análise de `FileScan parquet` mostrou três mecanismos complementares:

| Mecanismo | O que permite evitar |
| --- | --- |
| **Column Pruning** | Ler colunas que não participam da consulta |
| **Predicate Pushdown** | Processar dados que o leitor consegue descartar antecipadamente |
| **Partition Pruning** | Considerar diretórios/arquivos de partições fora do filtro |

Foram explorados ainda **Row Groups**, estatísticas min/max, *data skipping* e as diferenças entre `repartition()` (execução Spark) e `write.partitionBy()` (organização física no armazenamento). Um teste de particionamento por `cliente_id` evidenciou o risco de excesso de diretórios e arquivos pequenos.

### 4. Spark ↔ PostgreSQL com JDBC

Foi montado um laboratório com **Spark/Jupyter e PostgreSQL em containers Docker**, utilizando o driver JDBC do PostgreSQL.

```text
PostgreSQL (spark_db)
        ↑↓ JDBC
  Spark / PySpark
        ↓
  tabelas staging e datasets processados
```

Os experimentos incluíram escrita, leitura de volta, comparação de schema, `append`/`overwrite`, filtros SQL na origem e **uma tabela sintética de 2.000.000 de registros** gerada no PostgreSQL.

| Experimento | Resultado observado |
| --- | --- |
| Leitura JDBC sem particionamento | **1** Spark Partition |
| Leitura JDBC por `pedido_id`, com 4 partições | **4** Spark Partitions, com ~500 mil registros por partição |
| Filtro `status = 'concluido'` executado na origem | **500 mil** registros transferidos, distribuídos em ~125 mil por partição |
| `upperBound` inadequado | Partições desbalanceadas, apesar de manter 4 partições |
| `append` da mesma carga | Duplicação de registros na tabela de destino |

A principal conclusão: **`lowerBound`/`upperBound` definem referências para a divisão da faixa de valores da coluna JDBC, não a quantidade de linhas e nem um filtro da consulta.** Mais conexões ou partições também não garantem melhor desempenho: o PostgreSQL continua sendo uma fonte com recursos limitados.

## Organização do repositório

```text
.
├── notebooks/                # Exemplos e experimentos acompanhados nas aulas
│   ├── aula_01_fundamentos.ipynb
│   ├── ...
│   ├── aula_08_parquet.ipynb
│   ├── aula_09_jdbc.ipynb
│   └── CREATE TABLE public.sql # SQL de apoio e geração de massa JDBC
├── desafios/                 # Implementações e respostas dos desafios
│   ├── desafio_01_fundamentos,.ipynb
│   ├── ...
│   └── desafio_09_jdbc.ipynb
├── datasets/                 # CSVs de laboratório e dados gerados localmente
│   ├── 01_fundamentos/
│   ├── ...
│   ├── 07_joins/
│   ├── 08_parquet/
│   └── 09_jdbc/
├── docs/                     # Explicações detalhadas das aulas e desafios
│   ├── README.md
│   ├── 01_fundamentos.md
│   ├── ...
│   ├── 09_jdbc_postgresql.md
│   ├── arquitetura_do_curso.md
│   └── glossario.md
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

> **Nota:** a estrutura resume as pastas e os arquivos usados na trilha. O nome `desafio_01_fundamentos,.ipynb` reproduz o arquivo enviado; é recomendável remover a vírgula numa futura padronização.

## Como executar localmente

**Pré-requisitos:** Docker e Docker Compose. A configuração utiliza Jupyter/PySpark e, nos módulos JDBC, PostgreSQL.

1. Clone o repositório e entre na pasta do projeto:

   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd <PASTA_DO_REPOSITORIO>
   ```

2. Configure as variáveis locais usadas pelo serviço PostgreSQL em um arquivo `.env` na raiz:

   ```dotenv
   POSTGRES_DB=spark_db
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=defina_uma_senha_local
   ```

   **Não versionar** o `.env` nem credenciais. O driver PostgreSQL JDBC deve estar disponível na imagem Spark; nos notebooks da Aula 09 ele é referenciado em `/opt/jdbc/postgresql.jar`.

3. Inicie os serviços **no terminal do host**, não dentro do container Spark:

   ```bash
   docker compose up -d --build
   docker compose ps
   ```

4. Abra o Jupyter pelo endereço `http://localhost:8888` ou use o VS Code conectado ao container Spark. Os notebooks usam caminhos sob `/workspace`.

   Dentro da rede Docker, o banco é acessado por `postgres:5432`; a partir do host, pela porta PostgreSQL publicada no Compose, geralmente `localhost:5432`.

5. Para reproduzir os exercícios JDBC de escala, execute no PostgreSQL o SQL de geração da tabela `public.fato_pedidos_large` disponível em [`notebooks/CREATE TABLE public.sql`](notebooks/CREATE%20TABLE%20public.sql). Esse script contém também comandos de apoio; revise as instruções antes de executá-lo por inteiro.

> **Segurança:** a configuração original do laboratório permite acesso local simplificado ao Jupyter. Não exponha portas e serviços sem autenticação diretamente à internet.

### Sobre os datasets grandes

Arquivos grandes gerados nos testes de escala são **mantidos fora do versionamento** via `.gitignore`. Isso mantém o repositório leve. Os datasets de grande volume devem ser regenerados localmente para reproduzir experimentos de particionamento, joins e desempenho.

## Próximos passos

A trilha continuará com a conclusão do **Desafio 04 — Filtros e Ordenação**, seguida da **Aula 10 — Performance**. Depois será desenvolvido o **Projeto Final Integrador**, no qual decisões de schema, joins, particionamento, qualidade, formato de armazenamento e integração JDBC serão tomadas dentro de um fluxo único.

```text
Fontes (CSV / JSON / PostgreSQL)
                ↓
         Spark / PySpark
                ↓
      ingestão e validação
      joins e processamento
      análise do Physical Plan
                ↓
       Parquet / PostgreSQL
                ↓
     SQL / dbt (quando aplicável)
                ↓
        consumo analítico
```

## Princípio central

> **Não basta fazer o pipeline funcionar. É preciso entender sua execução, validar os dados e justificar as decisões de engenharia.**

Este repositório representa uma jornada de aprendizagem aplicada — **não uma biblioteca pronta para produção** — e será atualizado conforme a evolução dos desafios e do projeto final.
