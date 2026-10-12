# Aula 04 — Filtros, ordenação e `limit`

> **Fontes:** [`aula_04_filtros_ordenacao.ipynb`](../notebooks/aula_04_filtros_ordenacao.ipynb) e [`desafio_04_filtros_ordenacao.ipynb`](../desafios/desafio_04_filtros_ordenacao.ipynb).
>
> **Status:** aula estudada e **Desafio 04 concluído**, após revisão dos filtros e da interpretação do plano físico. Os resultados do desafio registrados abaixo são os apresentados nas execuções compartilhadas durante a correção. Caso o notebook local ainda contenha saídas antigas de `explain(True)`, elas devem ser atualizadas antes do commit.

## 1. Filtros compostos com a API de colunas

A transformação `filter()` (ou `where()`) cria um novo DataFrame que representa apenas os registros que satisfazem uma condição. A operação é *lazy*: o Spark registra a transformação no plano e normalmente só a executa quando uma *action* solicita um resultado.

Para combinar condições sobre colunas Spark, utilizamos `F.col()` e os operadores:

| Operador | Significado | Exemplo |
| --- | --- | --- |
| `&` | AND — ambas as condições | `(idade >= 35) & (renda > 5000)` |
| `\|` | OR — uma das condições | `(cidade == "Salvador") \| (cidade == "Recife")` |
| `~` | NOT — nega a condição | `~(cidade == "Salvador")` |

Exemplo estudado:

```python
from pyspark.sql import functions as F

df_cidade = df.filter(
    (F.col("cidade") == "Salvador") |
    (F.col("cidade") == "Recife")
)
```

**Atenção à precedência:** `&` tem precedência maior que `|` nas expressões usadas aqui. Portanto, `A | B & C` representa `A | (B & C)`, não `(A | B) & C`. Use parênteses explícitos para expressar a regra de negócio. `and`, `or` e `not` do Python não substituem esses operadores na combinação de objetos `Column`.

No notebook da aula, trabalhamos com filtros por idade, renda, cidade e situação ativa; negação para selecionar registros fora de Salvador ou inativos; e combinações como **idade ≥ 30 OU renda ≥ 5.000 OU ativo**. Os planos mostraram que o Catalyst pode reorganizar expressões e acrescentar condições `isnotnull(...)` quando compatíveis com a semântica do filtro.

## 2. Filtros Narrow e ordenação global

### Por que `filter()` é normalmente Narrow?

Uma transformação **Narrow** permite que cada partição produza sua saída a partir dos seus próprios registros, sem exigir a redistribuição dos dados entre partições.

```text
Partição 0 ── filter ── registros válidos da partição 0
Partição 1 ── filter ── registros válidos da partição 1
Partição 2 ── filter ── registros válidos da partição 2
```

Filtrar não exige que registros de uma partição sejam comparados aos de outra. Por isso, `filter()` costuma ser menos custoso que operações que precisam reorganizar dados globalmente.

### Por que `orderBy()` pode ser Wide?

Para garantir uma **ordem global**, não basta ordenar os registros individualmente dentro de cada partição: é necessário que os intervalos de valores entre partições também respeitem a ordenação. Dependendo da distribuição já existente, o Spark pode executar algo semelhante a:

```text
FileScan
   ↓
Exchange (redistribuição por intervalos, se necessário)
   ↓
Sort (ordenação dentro das partições)
   ↓
resultado globalmente ordenado
```

No estudo da aula, analisamos planos com operadores como `Sort` e `Exchange`. Eles possuem papéis diferentes:

- **`Sort`**: ordena registros.
- **`Exchange`**: define uma nova distribuição; frequentemente marca uma fronteira de Shuffle.
- **Shuffle**: é o movimento/redistribuição de dados realizado para satisfazer essa distribuição.

**Não é correto afirmar que qualquer chamada a `orderBy()` sempre produzirá um Shuffle completo.** O resultado depende das partições, da operação seguinte e da estratégia física escolhida pelo Spark.

## 3. `limit()` não é sinônimo de `top N`

As operações abaixo não possuem a mesma semântica:

```python
df.limit(10)

df.orderBy(F.col("renda_mensal").desc()).limit(10)
```

Na primeira, o Spark retorna até dez registros, **sem garantir quais** quando não existe ordenação explícita. No notebook da aula, isso foi observado pela distinção entre `LocalLimit`/`GlobalLimit` no plano lógico e operadores físicos como `CollectLimit`.

Na segunda, procuramos os **dez maiores valores** de `renda_mensal`. O otimizador pode utilizar `TakeOrderedAndProject` (estratégia de *Top-K*) em vez de ordenar globalmente todos os registros com um `Sort` completo. Essa diferença apareceu de forma clara no desafio.

### Como funciona a ideia de Top-K?

Para obter os vinte maiores valores de um DataFrame distribuído em oito partições, uma estratégia possível é:

```text
8 Spark Partitions
       ↓
Top 20 candidatos locais em cada partição
       ↓
Até 160 candidatos para combinação
       ↓
Top 20 global
```

Todos os registros relevantes ainda precisam ser considerados para que o Spark determine os maiores. A economia vem de **não precisar ordenar e redistribuir integralmente todo o conjunto** quando um algoritmo de Top-K é aplicável.

## 4. O que foi feito no notebook da aula

Em [`aula_04_filtros_ordenacao.ipynb`](../notebooks/aula_04_filtros_ordenacao.ipynb), foram praticados:

1. **Filtros simples e compostos**, comparando idade, cidade, renda e campo booleano `ativo`.
2. **Expressões com `&`, `|` e `~`**, reforçando parênteses, negação e precedência.
3. **Ordenação pela renda**, com `orderBy()` e direção decrescente.
4. **Limitação de resultados**, distinguindo `limit(10)` de `orderBy(...).limit(10)`.
5. **Inspeção com `explain(True)`**, analisando planos lógicos e físicos e observando quando `Filter`, `Sort`, `Exchange`, `CollectLimit` e estratégias alternativas fazem sentido.
6. Encerramento da sessão Spark com `spark.stop()`.

A conclusão da aula foi que **o custo da operação não deve ser inferido apenas pelo nome da função**: o plano físico e as características dos dados são determinantes.

## 5. O que foi feito no Desafio 04 — Concluído

O desafio foi resolvido e revisado em sete partes. Inicialmente, ocorreram dois erros de lógica nos filtros; ambos foram corrigidos antes da aprovação.

### Parte A — Leitura e inspeção

O CSV `datasets/04_filtros_ordenacao/clientes.csv` foi carregado com cabeçalho e inferência de schema. Foram verificados:

```text
Registros: 300
Spark Partitions: 1
```

O `printSchema()` confirmou, entre outros tipos:

- `cliente_id` e `idade`: `integer`;
- `renda_mensal`: `double`;
- `cidade`: `string`;
- `ativo`: `boolean`;
- `data_cadastro`: `date`.

A análise destacou que conhecer o schema é importante para evitar comparações inválidas, erros de tipagem e interpretações equivocadas, especialmente em CSV, que não carrega nativamente um schema tipado como Parquet.

**Observação de revisão:** a instrução para exibir cinco linhas pode ser atendida com `show(5)` caso ainda não esteja registrada na versão final do notebook.

### Parte B — Filtro com AND

Requisito: encontrar clientes com **idade ≥ 35** e **renda mensal > 5.000**.

```python
df_filtro_and = (
    df.select("cliente_id", "nome", "idade", "cidade", "renda_mensal")
      .filter((F.col("idade") >= 35) & (F.col("renda_mensal") > 5000))
)
```

**Resultado final: 148 registros.**

Na primeira tentativa havia sido usado `idade > 35`, que excluía clientes com exatamente 35 anos e retornava 143 registros. A correção mostrou a importância da diferença entre `>` e `>=` na fidelidade à regra de negócio.

O filtro é **Narrow**: cada partição pode avaliar seus próprios registros sem precisar redistribuí-los.

### Parte C — Filtro com OR e AND

Regra: **(Salvador OU Recife) E idade ≥ 30**.

```python
df_filtro_and_or = (
    df.select("cliente_id", "nome", "idade", "cidade", "renda_mensal")
      .filter(
          ((F.col("cidade") == "Salvador") | (F.col("cidade") == "Recife"))
          & (F.col("idade") >= 30)
      )
)
```

**Resultado final: 76 registros.**

A primeira implementação agrupava os operadores de forma que a idade só fosse aplicada aos clientes de Recife, retornando 100 registros. A correção reforçou que **um código que executa sem erros pode representar a regra de negócio errada**.

### Parte D — Negação com `~`

Foi aplicada uma condição que exclui clientes da cidade de Salvador:

```python
df_not_salvador = df.filter(~(F.col("cidade") == "Salvador"))
```

```text
Antes do filtro: 300 registros
Depois do filtro: 200 registros
```

O operador `~` nega a expressão booleana; `&` exige as duas condições e `|` aceita ao menos uma. Para colunas com `null`, também é necessário considerar a lógica de três valores do SQL: negar uma comparação nula não a transforma automaticamente em `true`.

### Parte E — Ordenação e limite

Os clientes foram ordenados por `renda_mensal` decrescente e limitados aos dez primeiros:

```python
df_limit = (
    df.select("cliente_id", "nome", "cidade", "renda_mensal")
      .orderBy(F.col("renda_mensal").desc())
      .limit(10)
)

df_limit.show()
```

A execução confirmou dez linhas ordenadas: a maior renda apresentada foi **14.998,33**, seguida por **14.974,64** e **14.967,50**. O `show()` foi acrescentado na revisão para comprovar visualmente os resultados.

### Parte F — Interpretação do `explain(True)`

**Plano do filtro.** Foi observado um fluxo semelhante a:

```text
FileScan CSV
     ↓
Filter
     ↓
Project
```

O *Optimized Logical Plan* incluía `isnotnull(idade)` e `isnotnull(renda_mensal)` e projetava apenas as colunas necessárias. Isso exemplifica otimizações lógicas e redução de colunas.

**Atenção à saída salva:** na captura do plano do filtro, ainda aparecia `idade > 35` porque a célula do `explain(True)` não havia sido reexecutada depois da correção para `>= 35`. Antes de versionar, é importante executar novamente essa célula para que **código e saída documentem a mesma lógica**.

**Plano do Top-10.** O *Physical Plan* observado foi:

```text
TakeOrderedAndProject(limit=10, orderBy=[renda_mensal DESC])
     ↓
FileScan CSV
```

Não havia `Exchange` explícito nem um operador `Sort` separado nesse plano. Portanto, **não foi demonstrada uma ordenação global completa com Shuffle**. O Spark escolheu uma estratégia de Top-K para essa consulta.

> Um plano explica **como o Spark pretende executar** o processamento. Para confirmar movimentação efetiva de dados, duração e gargalos, deve-se complementar `explain(True)` com as informações de execução do Spark UI.

### Parte G — Raciocínio de Engenharia de Dados

O desafio terminou com o cenário hipotético de **50 milhões de registros em 8 partições**, buscando os vinte clientes de maior renda. A análise consolidou que:

- filtros podem ser aplicados em paralelo, partição por partição;
- descobrir um Top-20 global exige comparar candidatos entre partições, mas não necessariamente executar uma ordenação global completa;
- mais partições não significam automaticamente menor tempo de execução;
- `Exchange` aparece quando o plano demanda uma distribuição diferente da existente, mas não pode ser presumido apenas pela presença de `orderBy()` no código;
- `TakeOrderedAndProject` explica por que o Top-K pode evitar os custos de um Shuffle completo dos dados.

**Resultado da revisão:** **Desafio 04 aprovado, avaliação final de 9,0/10** (evolução em relação à avaliação inicial de 7,8/10). Os erros nos filtros foram corrigidos e a compreensão de Top-K melhorou. Restaram apenas ajustes de sincronização de saídas e precisão na explicação da Parte F.

## 6. O que esta aula ensinou na prática

1. **Regra de negócio vem antes da sintaxe:** pequenos erros de comparação e parênteses alteram o conjunto de resultados sem gerar exceções.
2. **`filter()` normalmente é Narrow:** o processamento não precisa redistribuir registros para aplicar uma condição local.
3. **Ordenação global pode ser cara:** normalmente exige coordenação entre partições, mas o otimizador pode escolher outras estratégias conforme a consulta.
4. **`limit()` não define os maiores valores:** para um Top-N, é preciso especificar `orderBy()`.
5. **Ler o Physical Plan impede conclusões automáticas:** `TakeOrderedAndProject` não equivale a `Sort` global com Shuffle completo.
6. **Reexecutar células após correções é parte da qualidade do notebook:** resultados e planos desatualizados podem contradizer o código final.

**Próximo módulo:** [05 — Partições](05_particoes.md). A etapa seguinte da trilha, após a conclusão das Aulas 01–09, é a Aula 10 — Performance.
