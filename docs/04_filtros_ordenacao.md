# Aula 04 — Filtros, ordenação e `limit`

> **Fontes:** [`aula_04_filtros_ordenacao.ipynb`](../notebooks/aula_04_filtros_ordenacao.ipynb) e [`desafio_04_filtros_ordenacao.ipynb`](../desafios/desafio_04_filtros_ordenacao.ipynb).

## 1. Filtros compostos com a API de colunas

`filter()` cria um DataFrame em que permanecem apenas registros que atendem à condição. Usamos `F.col()` para representar as colunas do Spark e operadores `&` (**AND**), `|` (**OR**) e `~` (**NOT**) com parênteses para controlar a precedência.

```python
df_cidade = df.filter(
    (F.col("cidade") == "Salvador") |
    (F.col("cidade") == "Recife")
)
```

No notebook também foi aplicado `idade >= 35`, um filtro composto por **idade >= 30 OU renda >= 5000 OU ativo**, além de condições negativas (`~`) para clientes inativos e pessoas fora de Salvador. Na análise do plano, o Spark pôde normalizar algumas expressões, como `ativo = true`, e acrescentar verificações de nulidade quando necessárias.

**Cuidados:** `&` e `|` são operadores de expressões `Column`; use parênteses em cada comparação. Não substitua por `and`/`or` do Python ao combinar colunas Spark. Para campos booleanos, prefira `F.col("ativo") == True` ou apenas `F.col("ativo")` em vez da string `"true"`.

## 2. Filtro Narrow x ordenação Wide

Um filtro normal é **Narrow** porque cada partição pode eliminar seus próprios registros sem pedir dados de outras partições:

```text
P0 → filter → saída de P0
P1 → filter → saída de P1
P2 → filter → saída de P2
```

Já uma ordenação **global** como `orderBy(F.col("renda_mensal").desc())` precisa estabelecer a ordem entre registros de diferentes partições. No plano físico do notebook apareceram `Sort` e `Exchange`, indicação de redistribuição ou coordenação da ordenação.

```text
FileScan
   ↓
Exchange / Shuffle (quando necessário)
   ↓
Sort
   ↓
registros ordenados globalmente
```

`Sort` ordena os dados; `Exchange` estabelece a distribuição necessária. Não são o mesmo trabalho.

## 3. `limit()` não é sinônimo de ordenação

O notebook inspecionou:

```python
df.limit(10)
df.orderBy(F.col("renda_mensal").desc()).limit(10)
```

No primeiro caso, o plano mostrou `LocalLimit`, `GlobalLimit` e, fisicamente, `CollectLimit 10`. **Sem `orderBy` não existe garantia de quais dez linhas serão escolhidas**. Já o segundo padrão descreve um *top 10* pela renda; conforme a otimização e a versão do Spark, operadores como `TakeOrderedAndProject` podem substituir uma ordenação global completa.

## O que foi feito no notebook da aula

Foram filtrados clientes por idade, cidade, renda e situação ativa; usados operadores de negação para excluir registros; ordenados clientes por renda mensal decrescente; e comparados `limit(10)` com `orderBy(...).limit(10)` através de `explain(True)`. O notebook encerra a sessão com `spark.stop()`.

## Sobre o desafio 04 enviado

**O arquivo `desafio_04_filtros_ordenacao.ipynb` do ZIP tem zero células.** Portanto, não há nele código, resultados ou respostas para comentar. Isso não significa que o exercício não tenha sido realizado em outro momento; significa apenas que **esta cópia** está vazia. Não atribuí exercícios inexistentes ao arquivo.

Como roteiro de revisão, os conteúdos efetivamente demonstrados no notebook da aula são: filtros com `AND`/`OR`/`NOT`, `Filter` no plano, comparação com ordenação global e diferenças de `limit` e *top N*. Esses itens não substituem um desafio que não foi fornecido.

## Para revisar

- Filtros são tipicamente Narrow; ordenação global pode exigir Shuffle.
- O `orderBy` define a ordenação; o `limit` sozinho não define *top N*.
- `explain(True)` permite separar `Filter`, `Sort`, `Exchange` e `CollectLimit`.

**Próximo módulo:** [05 — Partições](05_particoes.md).
