# Aula 07 — Joins, cardinalidade e estratégias físicas

> **Fontes:** [`aula_07_joins.ipynb`](../notebooks/aula_07_joins.ipynb) e [`desafio_07_joins.ipynb`](../desafios/desafio_07_joins.ipynb).

## 1. O que determina o resultado de um join?

Um join combina registros de dois DataFrames segundo uma **condição de correspondência**. Antes de decidir `how="inner"` ou `how="left"`, precisamos saber: qual é a chave correta, quais IDs são válidos e quantas correspondências cada linha pode encontrar?

A aula utilizou três fontes de tamanho pequeno, com anomalias intencionais: `clientes.csv`, `pedidos.csv` e `pagamentos.csv`. Para estudar o plano físico em escala, houve ainda leitura de `large_csvs/clientes_large.csv` e `large_csvs/pedidos_large.csv`.

## 2. Tipos de join usados no notebook

| Tipo | Comportamento |
|---|---|
| `inner` | Retorna combinações com correspondência nos dois lados |
| `left` | Preserva linhas da esquerda; colunas direitas ficam `NULL` sem match |
| `right` | Preserva linhas da direita |
| `full` | Preserva não correspondências dos dois lados |
| `left_semi` | Retorna **somente colunas e linhas da esquerda** que têm algum match |
| `left_anti` | Retorna **somente linhas/colunas da esquerda** sem match à direita |

Exemplos efetivamente usados:

```python
# Clientes sem pedidos
df_clientes.join(df_pedidos, on="cliente_id", how="left_anti")

# Pedidos cujo cliente não existe
df_pedidos.join(df_clientes, on="cliente_id", how="left_anti")
```

No `left_semi`, múltiplas correspondências à direita não duplicam a linha da esquerda: ele testa existência e **não carrega campos da direita**. No `left_anti`, a regra prática é **colocar à esquerda a tabela cujas linhas você quer investigar**.

## 3. Cardinalidade e multiplicação de linhas

**1:1** significa no máximo um par correspondente por chave nos dois lados. **1:N** significa que um cliente pode ter vários pedidos. **N:N** surge quando ambos os lados repetem a chave.

```text
esquerda: chave 10 duas vezes
 direita: chave 10 três vezes

join → 2 × 3 = 6 linhas
```

Um join N:N não é necessariamente um erro, mas pode **inflar `SUM(valor_total)`** caso a métrica de uma tabela seja repetida pelas múltiplas combinações. A validação prévia usada foi `groupBy(chave).agg(F.count("*"))` com `filter(count > 1)`.

## 4. Qualidade de dados por `left_anti`

O notebook executou verificação de clientes duplicados, quantidade de pedidos por cliente e identificação de pedidos órfãos (pedidos cujo cliente não existe). Também procurou pagamentos sem pedido, usando `pagamentos LEFT ANTI pedidos`.

No desafio, a checagem de `clientes` duplicados retornou vazia; a frequência de pedidos por cliente confirmou a relação 1:N. A aula registrou **166 pedidos órfãos** no cenário pequeno. São testes de **integridade referencial** que devem preceder métricas agregadas sobre joins.

## 5. Aliases e joins encadeados

O notebook nomeou os lados como `cli`, `ped` e `pag` para resolver nomes ambíguos. A cadeia foi:

```text
clientes cli
     ↓ INNER por cliente_id
pedidos ped
     ↓ LEFT por pedido_id
pagamentos pag
     ↓
cliente_id, estado, pedido_id, valor_total,
pagamento_id, forma_pagamento, valor_pago
```

Exemplo simplificado:

```python
clientes = df_clientes.alias("cli")
pedidos = df_pedidos.alias("ped")
pagamentos = df_pagamentos.alias("pag")

base = clientes.join(pedidos, clientes.cliente_id == pedidos.cliente_id, "inner")
final = base.join(pagamentos, base.pedido_id == pagamentos.pedido_id, "left")
```

`LEFT` preserva o **resultado intermediário da esquerda**, inclusive pedidos sem pagamento, cujo `pagamento_id` aparece `NULL`.

Na verificação do desafio, o primeiro join e o pipeline final apresentaram **4.834 linhas**. A checagem encontrou pagamentos duplicados para `pedido_id=20082` e `pedido_id=20001`. Eles **não multiplicaram** esse join final porque não correspondiam a pedidos válidos presentes na relação intermediária. A lição é que duplicidade só multiplica linhas **se a chave duplicada encontrar correspondência no outro lado**.

## 6. BroadcastHashJoin x SortMergeJoin

### BroadcastHashJoin

Quando um lado é suficientemente pequeno (considerando estatísticas/tamanho em bytes e regras da estratégia), Spark pode transmitir esse lado para os executores e construir uma hash table:

```text
clientes pequenos → BroadcastExchange → cópia acessível nos executores
pedidos grandes  → leitura das partições locais + busca na hash
```

O plano pode mostrar `BroadcastHashJoin [...], [...], Inner, BuildRight`: `BuildRight` quer dizer que o **lado direito daquele join específico** fornece a tabela hash. Não quer dizer que a direita é a saída do join inteiro. Um `F.broadcast(df_pequeno)` precisa ser passado **ao join** para influenciar sua escolha; aplicar o hint depois de construir o join não o altera retroativamente.

### SortMergeJoin

Com dois lados grandes, um plano observado no notebook usou:

```text
lado A: FileScan → Exchange hashpartitioning(chave) → Sort
lado B: FileScan → Exchange hashpartitioning(chave) → Sort
                                  ↓
                             SortMergeJoin
```

Os **dois Exchanges** redistribuem as chaves dos dois lados; os dois `Sort` ordenam dentro das novas partições; o operador de merge compara os registros ordenados. São operações distintas. Broadcast pode evitar esse Shuffle bilateral, mas também consome memória ao replicar o lado pequeno.

## 7. Join com chave composta

O desafio criou DataFrames mínimos com `(cliente_id, estado)` e mostrou:

```text
Join somente por cliente_id → 4 linhas
Join por cliente_id E estado → 2 linhas
```

Uma chave incompleta gera correspondências incorretas; em um projeto real isso altera faturamento, contagens e integridade. As duas condições devem ser combinadas com `&`.

## O que foi feito no notebook da aula

Foram praticados `inner`, `left`, `left_semi` e `left_anti`, identificação de nulos após left join, checagens de cardinalidade/órfãos, aliases e joins com três tabelas. `explain(True)` foi usado para contrastar joins em fontes pequenas (broadcast) e grandes (`SortMergeJoin` com dois Exchanges). O notebook também contém a tentativa de chamar `F.broadcast(df_clientes)` **separadamente** do join: esse trecho isolado não força broadcast naquele join; a correção apareceu no desafio.

## O que foi feito no desafio 07

A parte A validou unicidade e multiplicidade; B e C usaram `semi`/`anti` para presença e integridade; D integrou clientes/pedidos/pagamentos; E verificou multiplicação de linhas; F criou o join por chave composta; G estudou `SortMergeJoin` de dois CSVs grandes e usou corretamente `F.broadcast(df_clientes)` **dentro** da construção do join com pedidos grandes.

O desafio registrou resultados concretos: **4.834 linhas** nos dois estágios do pipeline, duplicidades em pagamentos órfãos (IDs **20082** e **20001**) e **4 versus 2 linhas** ao corrigir a chave composta. Esses números pertencem ao dataset de exercícios, não são invariantes da API.

## Para revisar

- Primeiro determine cardinalidade e qualidade das chaves; só depois escolha estratégia de join.
- `left_semi`/`left_anti` são especialmente úteis para validação de presença/ausência.
- `BuildLeft` e `BuildRight` se referem ao par de entradas do join físico atual.
- `Exchange` redistribui; `Sort` ordena; `SortMergeJoin` combina.

**Próximo módulo:** [08 — Parquet](08_parquet.md).
