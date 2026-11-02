# Spark Práticas — Apache Spark e PySpark para Engenharia de Dados

Reuni neste repositório os materiais e exercícios que desenvolvi durante meus estudos de Apache Spark e PySpark. Foram dez aulas, dez desafios individuais e um desafio final, voltados ao uso de Spark em Engenharia de Dados.

Comecei pela leitura de arquivos CSV e pelas transformações de DataFrames. Depois, trabalhei com joins, agregações, Parquet e integração com PostgreSQL. A análise dos planos de execução acompanhou esses exercícios, junto com os testes de particionamento e a consulta ao Spark UI.

## Ambiente utilizado

Os notebooks foram desenvolvidos em **Python 3.11 e PySpark 3.5.1**, com JupyterLab em Docker. O laboratório também usa **PostgreSQL 16**, acesso por JDBC e arquivos Parquet.

O [Dockerfile](Dockerfile) instala as [dependências Python](requirements.txt) e o driver PostgreSQL em `/opt/jdbc/postgresql.jar`. Os serviços, as portas e o volume do banco estão definidos no [Docker Compose](docker-compose.yml).

## Organização dos arquivos

```text
.
├── notebooks/                   # Aulas 01 a 10
├── desafios/
│   ├── README.md                # Índice dos exercícios
│   ├── desafio_01_fundamentos.ipynb
│   ├── ...
│   ├── desafio_10_performance.ipynb
│   └── desafio_11/
│       ├── ENUNCIADO_DESAFIO_FINAL.md
│       ├── desafio_11_final.ipynb
│       └── arquivos_parquet/    # Dados gerados pelo integrador
├── docs/                        # Explicações das aulas e glossário
├── datasets/                    # Dados dos exercícios
│   └── desafio_final_integrador/
│       ├── ENUNCIADO.md
│       ├── dados_desafio_final.zip
│       └── dados/               # CSVs extraídos localmente
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Aulas

O curso tratou da construção de DataFrames, da execução distribuída e dos cuidados necessários para validar e armazenar os resultados. Cada aula tem um notebook e um capítulo no [caderno de estudos](docs/README.md).

| Aula e notebook | Conteúdo | Documentação |
| --- | --- | --- |
| [01 — Fundamentos](notebooks/aula_01_fundamentos.ipynb) | SparkSession, schemas, DataFrames, actions e Lazy Evaluation | [Fundamentos](docs/01_fundamentos.md) |
| [02 — Lineage e DAG](notebooks/aula_02_lineage_dag.ipynb) | Dependências, Catalyst e planos de execução | [Lineage e DAG](docs/02_lineage_dag.md) |
| [03 — Transformações](notebooks/aula_03_transformacoes.ipynb) | `select`, `drop`, `withColumn`, aliases e Column Pruning | [Transformações](docs/03_transformacoes.md) |
| [04 — Filtros e ordenação](notebooks/aula_04_filtros_ordenacao.ipynb) | Expressões booleanas, `orderBy` e `limit` | [Filtros e ordenação](docs/04_filtros_ordenacao.md) |
| [05 — Partições](notebooks/aula_05_particoes.ipynb) | Paralelismo, `repartition`, `coalesce`, Narrow/Wide e Shuffle | [Partições](docs/05_particoes.md) |
| [06 — Agregações](notebooks/aula_06_agregacoes.ipynb) | `groupBy`, métricas condicionais, `countDistinct` e HashAggregate | [Agregações](docs/06_agregacoes.md) |
| [07 — Joins](notebooks/aula_07_joins.ipynb) | Cardinalidade, integridade referencial e estratégias de join | [Joins](docs/07_joins.md) |
| [08 — Parquet](notebooks/aula_08_parquet.ipynb) | Armazenamento colunar, pruning, Row Groups e small files | [Parquet](docs/08_parquet.md) |
| [09 — JDBC e PostgreSQL](notebooks/aula_09_jdbc.ipynb) | Leitura e escrita, particionamento JDBC e tamanho dos lotes | [JDBC e PostgreSQL](docs/09_jdbc_postgresql.md) |
| [10 — Performance](notebooks/aula_10_performance.ipynb) | Data Skew, BroadcastHashJoin, SortMergeJoin, AQE, cache e Spark UI | [Performance](docs/10_performance.md) |

## Desafios

Depois de cada aula, desenvolvi um exercício sobre o conteúdo estudado. Os dez desafios estão concluídos, com código, resultados e respostas nos notebooks. O [índice dos desafios](desafios/README.md) descreve os exercícios com mais detalhes.

| Desafio | Tema |
| --- | --- |
| [01](desafios/desafio_01_fundamentos.ipynb) | Fundamentos e primeiras transformações |
| [02](desafios/desafio_02_lineage_DAG.ipynb) | Lineage, DAG e planos de execução |
| [03](desafios/desafio_03_transformacoes.ipynb) | Projeções e criação de colunas |
| [04](desafios/desafio_04_filtros_ordenacao.ipynb) | Filtros e ordenação |
| [05](desafios/desafio_05_particoes.ipynb) | Particionamento e Shuffle |
| [06](desafios/desafio_06_agregacoes.ipynb) | Agregações distribuídas |
| [07](desafios/desafio_07_joins.ipynb) | Joins e qualidade de dados |
| [08](desafios/desafio_08_parquet.ipynb) | Parquet e leitura seletiva |
| [09](desafios/desafio_09_jdbc.ipynb) | Integração com PostgreSQL |
| [10](desafios/desafio_10_performance.ipynb) | Diagnóstico de performance |

## Desafio Final Integrador

Para encerrar o curso, desenvolvi um pipeline com dados de um marketplace fictício. O [notebook final](desafios/desafio_11/desafio_11_final.ipynb) parte de CSVs sintéticos e passa por cinco etapas:

1. Leitura das fontes e diagnóstico dos dados.
2. Validação de quantidades e referências, joins e cálculo dos indicadores.
3. Escrita em Parquet por `ano_mes` e análise da leitura com filtros.
4. Persistência no PostgreSQL e comparação dos dados recuperados por JDBC.
5. Testes de particionamento, estratégias de join, AQE e cache, com análise dos planos e das medições disponíveis.

O [enunciado](desafios/desafio_11/ENUNCIADO_DESAFIO_FINAL.md) detalha as atividades. Os CSVs usados no integrador estão no [arquivo ZIP dos dados](datasets/desafio_final_integrador/dados_desafio_final.zip).

A carga inicial tinha **2.000.000 de pedidos, 20.000 clientes e 300 produtos**. Identifiquei **20.618 pedidos com quantidade menor ou igual a zero**, além das referências sem correspondência nas dimensões. Após filtrar as quantidades e integrar clientes e produtos, restaram **1.939.794 pedidos**.

Com os pedidos concluídos, calculei quantidade de pedidos, unidades vendidas e receita líquida por **mês, UF e categoria**. Os **54 registros agregados** foram gravados em `public.desafio_final_indicadores` e lidos novamente pelo Spark. As comparações das chaves nos dois sentidos e das três métricas não apontaram diferenças nos testes realizados.

Os pedidos tratados também foram salvos em Parquet. No plano de leitura, examinei a seleção de colunas e o filtro por mês. Nos testes de execução, comparei BroadcastHashJoin e SortMergeJoin, observei a distribuição por chave e reutilizei dados com cache.

A dimensão de produtos pequena justificou a escolha de BroadcastHashJoin nesse cenário. Já as medições disponíveis não permitem afirmar um ganho percentual de desempenho ou apontar um gargalo significativo: faltam métricas completas e comparáveis do Spark UI no notebook final. O desequilíbrio das partições foi observado, mas seu efeito sobre o tempo de execução não foi isolado.

## O que os exercícios reforçaram

Validar as chaves antes de um join é necessário para evitar a multiplicação de registros e a distorção dos indicadores. Também ficou clara a diferença entre distribuir o processamento com `repartition` e organizar os arquivos com `partitionBy`.

Na análise de performance, o plano mostra as operações previstas, enquanto as métricas ajudam a entender seus custos. A presença de Shuffle, o uso de cache ou a ativação do AQE precisam ser avaliados no contexto da consulta. Por isso, registrei os resultados disponíveis e as limitações das comparações.

## Como executar

É necessário ter Docker instalado. Execute os comandos abaixo no terminal do host, na raiz do projeto.

O banco e os dados do PostgreSQL não acompanham o repositório. Quem clonar o projeto ou atualizá-lo com `git pull` precisará preparar seu próprio banco, com as tabelas e os dados usados nos exercícios JDBC.

1. Clone o repositório e entre na pasta criada:

   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd <PASTA_DO_REPOSITORIO>
   ```

2. Crie um `.env` na raiz com `POSTGRES_DB`, `POSTGRES_USER` e `POSTGRES_PASSWORD`, usando valores compatíveis com as conexões dos notebooks. Mantenha esse arquivo apenas no ambiente local.

3. Construa a imagem e inicie os serviços:

   ```bash
   docker compose up -d --build
   docker compose ps
   ```

   Na primeira inicialização de um volume vazio, o PostgreSQL cria o banco definido em `POSTGRES_DB`. As tabelas e os dados dos exercícios precisam ser preparados separadamente. Se o volume já foi inicializado, mudar essa variável não cria outro banco. Em uma instalação fora do Compose, crie o banco e ajuste a URL JDBC. A [documentação da imagem PostgreSQL](https://github.com/docker-library/docs/blob/master/postgres/README.md#environment-variables) explica a inicialização.

4. Abra o JupyterLab em **http://localhost:8888**. As aulas estão em `/workspace/notebooks` e os desafios em `/workspace/desafios`. Para abrir um terminal no serviço Spark:

   ```bash
   docker compose exec spark bash
   ```

   O laboratório usa acesso simplificado ao Jupyter e foi configurado para uso local.

5. Extraia os CSVs do integrador:

   ```bash
   docker compose exec spark python -m zipfile -e datasets/desafio_final_integrador/dados_desafio_final.zip datasets/desafio_final_integrador
   ```

   O comando extrai `pedidos.csv`, `clientes.csv` e `produtos.csv` para `datasets/desafio_final_integrador/dados/`, mantendo os caminhos usados pelo notebook.

6. Para abrir o cliente SQL do PostgreSQL:

   ```bash
   docker compose exec postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
   ```

   Dentro da rede Docker, o endereço do banco é `postgres:5432`; no host, é `localhost:5432`. O arquivo `notebooks/CREATE TABLE public.sql` é um apoio local ignorado pelo Git. Para reproduzir a Aula 09, prepare as tabelas e a massa de dados usadas nos notebooks, incluindo `public.fato_pedidos_large` nos testes de volume.

A **Spark UI fica em http://localhost:4040** enquanto uma aplicação estiver ativa. O Compose publica essa porta apenas no endereço local do host.

Antes de repetir células com `overwrite` ou `append`, confira os destinos de escrita. Os Parquets do integrador ficam em `desafios/desafio_11/arquivos_parquet/`, separados do notebook.

Para encerrar os serviços e manter o volume do banco:

```bash
docker compose down
```

O curso e o Desafio Final Integrador estão concluídos. Os arquivos deste repositório registram os exercícios e as análises desenvolvidos durante os estudos.
