# Aula 07 — Joins

Datasets sintéticos para praticar joins em PySpark.

## Arquivos
- `clientes.csv` — 500 clientes.
- `pedidos.csv` — 5.000 pedidos.
- `pagamentos.csv` — pagamentos associados aos pedidos.

## Relações
- `clientes.cliente_id` -> `pedidos.cliente_id`
- `pedidos.pedido_id` -> `pagamentos.pedido_id`

## Situações intencionais
- Clientes 471 a 500 não possuem pedidos.
- Alguns pedidos usam `cliente_id` entre 501 e 520, inexistentes em `clientes.csv`.
- Alguns pedidos não possuem pagamento.
- Existem alguns pagamentos com `pedido_id` inexistente.

Esses casos existem de propósito para praticar `inner`, `left`, `right`, `full`, `left_semi`, `left_anti` e validação de qualidade de dados.

Dados totalmente fictícios.
