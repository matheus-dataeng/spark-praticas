# Aula 02 — Lineage, DAG e Catalyst Optimizer

> **Fontes:** [`aula_02_lineage_dag.ipynb`](../notebooks/aula_02_lineage_dag.ipynb) e [`desafio_02_lineage_DAG.ipynb`](../desafios/desafio_02_lineage_DAG.ipynb).

## 1. O código descreve dependências

No Spark, um DataFrame costuma ser derivado de outro. O encadeamento de operações forma um **lineage** (histórico de dependências) que o Spark pode usar para planejar e recomputar resultados. O modelo de execução também é representado por um **DAG** (*Directed Acyclic Graph*): um grafo direcionado sem ciclos que expressa dependências entre operações e estágios. **Lineage, DAG de execução e planos SQL não são exatamente a mesma estrutura**, mas todos ajudam a explicar como as etapas se relacionam.

No notebook foi criado um DataFrame com:

```python
df2 = (
    df.filter(F.col("idade") > 30)
      .select("nome", "idade", "cidade")
      .withColumn("idade_futura", F.col("idade") + 1)
)
```

O exemplo deixa explícitas três dependências: filtrar clientes, escolher os campos necessários e calcular `idade_futura`. O Spark não precisa tratar essas instruções como operações isoladas sobre cópias completas de uma tabela.

## 2. Como ler `explain(True)`

O notebook imprimiu os quatro planos que aparecem em `explain(True)`:

```text
Parsed Logical Plan
       ↓
Analyzed Logical Plan
       ↓
Optimized Logical Plan
       ↓
Physical Plan
```

**Parsed:** estrutura sintática da consulta, ainda podendo conter referências não resolvidas. **Analyzed:** nomes de colunas e tipos foram resolvidos, com inserção de conversões de tipo quando necessárias. **Optimized:** o Catalyst aplica regras que buscam simplificar e tornar o plano mais eficiente. **Physical:** o Spark escolhe operadores executáveis, como `Project`, `Filter`, `FileScan` e, nos módulos seguintes, `Exchange`, `HashAggregate` e joins físicos.

O **Physical Plan é lido visualmente de baixo para cima para acompanhar a entrada dos dados**, apesar de aparecer impresso com a raiz na parte superior.

## 3. Experimento: filtros separados x filtro combinado

Foram construídas duas consultas sobre `clientes.csv`:

```python
# A: três filtros encadeados
df_a = (df.filter(F.col("idade") > 30)
          .filter(F.col("ativo") == True)
          .filter(F.col("renda_mensal") > 5000))

# B: as três condições juntas
df_b = df.filter(
    (F.col("idade") > 30)
    & (F.col("ativo") == True)
    & (F.col("renda_mensal") > 5000)
)
```

No `Parsed Logical Plan`, a versão A contém `Filter` encadeados e a B já expressa uma condição composta. Nos planos otimizados, o Catalyst reorganiza essas condições e acrescenta testes de nulidade quando apropriado. **As duas formas podem resultar em execução equivalente**, mesmo quando sua árvore inicial é diferente. Portanto, código mais comprido não implica automaticamente processamento mais caro.

## O que foi feito no notebook da aula

Além do exemplo `df2`, foram executados `df_a.explain(True)`, `df_b.explain(True)` e `df2.explain(True)`, permitindo comparar planos sintáticos, otimizados e físicos. O campo `idade_futura` apareceu como expressão de `Project`; o filtro de idade pôde aparecer próximo à leitura física.

## O que foi feito no desafio 02

O desafio montou uma preparação de clientes ativos de **35 anos ou mais**, selecionando `cliente_id`, `nome`, `idade`, `cidade`, `renda_mensal`, criando `renda_anual = renda_mensal * 12` e classificando a renda em **Baixa** (até 4.000), **Média** (acima de 4.000 até 8.000) e **Alta** (acima de 8.000).

Depois foi criada uma versão alternativa (`df_final_catalyst`) mudando a ordem das operações; o notebook comparou `explain(True)` e registrou que as formas acabaram com planos essencialmente equivalentes. Nas perguntas, foram diferenciados plano analisado, otimizado e físico, e explicado por que executar cada transformação imediatamente limitaria as oportunidades de otimização.

**Atenção:** comparar planos é diferente de provar que todos os programas com operações em ordem diferente são equivalentes. Algumas transformações dependem da ordem, como cálculos após filtros, operações não determinísticas ou limites.

## Para revisar

- `explain(True)` ajuda a compreender a **transformação do plano**, não mede tempo de execução.
- O Catalyst atua principalmente na otimização lógica; escolha de operadores físicos e mecanismos de execução completam o processo.
- `Project` é seleção/projeção de expressões; `Filter` representa condições; `FileScan` é leitura física.

**Próximo módulo:** [03 — Transformações](03_transformacoes.md).
