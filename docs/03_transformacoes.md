# Aula 03 — Transformações de colunas

> **Fontes:** [`aula_03_transformacoes.ipynb`](../notebooks/aula_03_transformacoes.ipynb) e [`desafio_03_transformacoes.ipynb`](../desafios/desafio_03_transformacoes.ipynb).

## 1. Selecionar, descartar e transformar

Com `select()` escolhemos colunas e expressões que devem compor o novo DataFrame. Com `drop()` especificamos campos a retirar. Em ambos os casos, o Spark pode reduzir o número de colunas efetivamente necessárias na leitura, uma otimização conhecida como **Column Pruning**.

No notebook, o CSV `03_transformacoes/clientes.csv` incluía `cliente_id`, `nome`, `idade`, `cidade`, `estado`, `profissao`, `renda_mensal`, `sexo`, `ativo` e `data_cadastro`. Dois exemplos utilizados foram:

```python
df_clientes = df.select("cliente_id", "nome", "cidade", "renda_mensal")
df_sem_dados_sensiveis = df.drop("sexo", "estado")
```

O `explain(True)` de `df_clientes` mostrou `FileScan csv` com apenas as quatro colunas escolhidas, ilustrando **Column Pruning**. `drop()` não deve ser confundido com garantia de anonimização: a remoção depende do novo DataFrame ser o utilizado nas etapas seguintes e de não haver outras informações identificáveis.

## 2. `withColumn()` e encadeamento

`withColumn(nome, expressao)` **cria uma coluna ou substitui uma coluna de mesmo nome no novo DataFrame**. A expressão é avaliada sobre a saída das transformações anteriores na cadeia.

```python
df_tratado = (
    df.withColumn("renda_anual", F.col("renda_mensal") * 12)
      .withColumn("idade", F.col("idade") + 5)
      .withColumn(
          "maior_idade",
          F.when(F.col("idade") >= 18, "Sim").otherwise("Não")
      )
)
```

Esse exemplo, retirado do notebook, é útil para observar **dependência entre transformações**: a coluna `maior_idade` utiliza a `idade` depois do acréscimo de cinco anos. A ordem, aqui, modifica o significado do resultado.

## 3. `withColumnRenamed()` e `alias()`

O notebook renomeou `cliente_id` para `id`, `nome` para `cliente` e `renda_mensal` para `salario` com `withColumnRenamed()`. Em outra célula, usou `F.col("nome").alias("cliente")` dentro de `select()`.

```python
# Renomeia uma coluna no DataFrame retornado
df.withColumnRenamed("nome", "cliente")

# Define o nome de uma expressão projetada
df.select(F.col("nome").alias("cliente"), F.col("idade"))
```

A diferença não é absoluta — ambos produzem nomes de saída — mas o `alias()` é especialmente conveniente em expressões calculadas, agregações e `select()`. Nos planos, renomeações aparecem como `Project [...] AS ...`.

## 4. Fusão de projeções

Uma sequência de `select`, renomeações e `withColumn` pode gerar vários `Project` no plano analisado. O Catalyst tenta eliminar etapas redundantes e combinar projeções quando isso preserva o significado. É por isso que observar somente a quantidade de chamadas no código não basta para estimar custo.

## O que foi feito no notebook da aula

Foram executados exemplos de `select`, `drop`, `withColumn`, `withColumnRenamed` e `alias`, com `show()`, `printSchema()` e vários `explain(True)`. O notebook demonstrou a leitura seletiva de colunas e a diferença entre alterar uma expressão e renomeá-la.

## O que foi feito no desafio 03

O desafio criou `df_cliente_tratado` selecionando seis campos, renomeando `cliente_id` para `id_cliente`, calculando renda anual, aumentando a idade em dois anos e classificando renda por faixas com `when`/`otherwise`. Em seguida construiu `df_relatorio`, projetando `id`, `cliente`, `cidade`, `renda` e `renda_anual` por meio de `alias()`.

As respostas discutiram os `Project` do Optimized Logical Plan, a impossibilidade de calcular `idade_futura` a partir de uma coluna que já não está disponível após `select()`, e a vantagem de selecionar cedo as oito colunas necessárias em um CSV hipotético de 150 colunas. **Refinamento importante:** `withColumn` não modifica o DataFrame original; devolve um novo DataFrame mesmo quando a variável recebe o mesmo nome.

## Para revisar

- O DataFrame à direita de cada transformação define quais colunas existem no passo seguinte.
- Use `select()` cedo para explicitar o conjunto necessário de colunas.
- `alias()` renomeia expressões; `withColumnRenamed()` renomeia um campo no schema resultante.
- O plano otimizado pode conter menos `Project` do que a sequência inicial sugeriria.

**Próximo módulo:** [04 — Filtros e ordenação](04_filtros_ordenacao.md).
