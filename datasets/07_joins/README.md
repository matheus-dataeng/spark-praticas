# Aula 07 — Joins

Esta pasta contém os dados sintéticos usados nos exercícios de joins da Aula 07.

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

As inconsistências foram incluídas para exercitar os joins `inner`, `left`, `right`, `full`, `left_semi` e `left_anti`, além das verificações de qualidade. Todos os dados são fictícios.
