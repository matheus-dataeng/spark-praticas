CREATE TABLE public.staging_pedidos (
    pedido_id SERIAL PRIMARY KEY,
    cliente_id INT NOT NULL,
    data_pedido TIMESTAMP NOT NULL,
    status VARCHAR(50),
    canal VARCHAR(20),
    valor_total NUMERIC(10, 2)
)

SELECT COUNT(*)
FROM public.staging_pedidos;


SELECT * FROM public.staging_pedidos;

DROP TABLE IF EXISTS public.fato_pedidos_large;

CREATE TABLE public.fato_pedidos_large AS
SELECT
    gs AS pedido_id,
    ((gs - 1) % 300000) + 1 AS cliente_id,
    DATE '2024-01-01' + ((gs - 1) % 730)::integer AS data_pedido,
    CASE
        WHEN gs % 4 = 0 THEN 'concluido'
        WHEN gs % 4 = 1 THEN 'enviado'
        WHEN gs % 4 = 2 THEN 'pendente'
        ELSE 'cancelado'
    END AS status,
    CASE
        WHEN gs % 3 = 0 THEN 'site'
        WHEN gs % 3 = 1 THEN 'app'
        ELSE 'marketplace'
    END AS canal,
    ROUND((50 + (gs % 4950))::numeric, 2) AS valor_total
FROM generate_series(1, 2000000) AS gs;

ALTER TABLE public.fato_pedidos_large
ADD PRIMARY KEY (pedido_id);

ANALYZE public.fato_pedidos_large;

SELECT COUNT(*)
FROM public.fato_pedidos_large;

SELECT status, COUNT(*)
FROM public.fato_pedidos_large
GROUP BY status;

CREATE TABLE public.pedidos_concluidos_batch (
    pedido_id INTEGER,
    cliente_id INTEGER,
    valor_total NUMERIC(38,18)
);

SELECT COUNT(*)
FROM public.pedidos_concluidos_batch;

CREATE TABLE public.desafio_pedidos_concluidos(
    pedido_id SERIAL PRIMARY KEY, 
    cliente_id INT NOT NULL, 
    valor_total NUMERIC (10, 2)
)

SELECT COUNT(*) FROM public.desafio_pedidos_concluidos

SELECT
    column_name,
    data_type,
    is_nullable,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'desafio_pedidos_concluidos'
ORDER BY ordinal_position;