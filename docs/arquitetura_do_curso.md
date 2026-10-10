# Arquitetura de aprendizado e relação entre os módulos

Esta trilha foi praticada em notebooks Jupyter sobre **PySpark em Docker**, inicialmente usando CSV e depois integrando **Parquet e PostgreSQL via JDBC**. Os exemplos procuram conectar APIs simples (`select`, `filter`, `join`) aos operadores efetivamente observados em `explain(True)`.

```text
CSV / tabelas PostgreSQL
         ↓
      DataFrame
         ↓
  select / filter / withColumn       → projeção/filtro (Narrow)
         ↓
  groupBy / join / orderBy           → dependências por chave, Sort e/ou Shuffle
         ↓
  Parquet / PostgreSQL JDBC
```

## Sequência do curso

```text
01 Fundamentos       ── DataFrame, Column, action
02 Lineage / DAG     ── Catalyst e quatro planos
03 Transformações    ── projeções, renomeação, criação de colunas
04 Filtros / Order   ── Filter, Sort, CollectLimit
05 Partições         ── Tasks, Narrow/Wide, Exchange/Shuffle
06 Agregações        ── HashAggregate partial/final, countDistinct
07 Joins             ── cardinalidade, left_anti, Broadcast/SortMerge
08 Parquet           ── colunar, Row Group, pruning, small files
09 JDBC / PostgreSQL ── paralelismo na ingestão, bounds, lotes
10 Performance       ── próxima aula; não documentada nestes anexos
11 Projeto Final     ── futuro integrador; ainda não realizado
```

## Laboratório pequeno x laboratório grande

Os módulos 01–06 privilegiam CSVs com volume suficiente para inspecionar os resultados e planos sem perder a lógica. A Aula 07 inclui versões grandes de `clientes` e `pedidos` para comparar estratégias físicas de join. A Aula 09 criou uma **tabela PostgreSQL com 2 milhões de linhas**, permitindo observar diferenças reais entre uma leitura JDBC de uma partição e outra com quatro.

Arquivos grandes gerados localmente são apropriados para o laboratório, mas não precisam ser versionados no GitHub; incluir os caminhos no `.gitignore` ajuda a manter o repositório leve. Para reprodutibilidade, a geração SQL com `generate_series` da Aula 09 está registrada em `notebooks/CREATE TABLE public.sql`.

## O que observar ao avaliar um pipeline

1. **Semântica:** quais linhas e colunas são realmente necessárias? A chave de join está correta?
2. **Plano:** qual operador físico foi escolhido? Existe `Exchange` e Shuffle? Há agregação parcial?
3. **Distribuição:** as Spark Partitions são numerosas e equilibradas o suficiente? Há skew?
4. **Origem:** a leitura JDBC é paralela? Os filtros e projeções chegam ao PostgreSQL? O banco aguenta as conexões?
5. **Destino:** Parquet colunar e particionado conforme consultas? Há small files? O `append` duplica registros?

## Relação com um pipeline de Engenharia de Dados

O curso enfatiza Spark como mecanismo de **ingestão e processamento técnico**. O uso posterior de SQL/dbt para regras analíticas é uma **direção arquitetural planejada**, não algo implementado em todos os notebooks aqui descritos.

```text
fontes → Spark / preparação técnica → Parquet / Data Lake ou PostgreSQL
                                              ↓
                                     SQL / dbt (projeto futuro)
```
