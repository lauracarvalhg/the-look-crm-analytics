-- CRIAÇÃO DA CAMADA SILVER
-- TABELA: stg_order_items
-- OBJETIVO: Filtrar itens de pedidos válidos,
--           calcular margem por item e enriquecer
--           com dados de categoria e custo
--           via JOIN com products
-- FONTE: bigquery-public-data.thelook_ecommerce.order_items
--        bigquery-public-data.thelook_ecommerce.products
-- DESTINO: crm_analytics.stg_order_items
-- DECISÃO DE NEGÓCIO: campo cost não existe em
--           order_items — buscado via JOIN em products
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-15



CREATE OR REPLACE TABLE `integracao-de-dados-the-look.crm_staging.stg_order_items` AS

-- CTE 1: order_items_filtrados
-- OBJETIVO: Filtrar itens válidos e calcular
--           margem unitária por item
-- JOIN com products: necessário pois o campo cost
--           não existe em order_items — descoberto
--           na fase de auditoria
-- LEFT JOIN: garante que itens sem produto cadastrado
--           não sejam perdidos — custo fica NULL
-- NULLIF(sale_price, 0): evita divisão por zero
--           no cálculo de margem percentual

WITH order_items_filtrados AS (
  SELECT
    oi.id                                             AS order_item_id,  -- chave primária do item
    oi.order_id,                                                         -- chave estrangeira para orders
    oi.user_id,                                                          -- chave estrangeira para users
    oi.product_id,                                                       -- chave estrangeira para products
    oi.sale_price,                                                       -- preço de venda do item
    oi.status,                                                           -- status do item
    oi.created_at,                                                       -- data do item

    -- Campos enriquecidos via JOIN com products
    p.cost,                                                              -- custo do produto
    p.category,                                                          -- categoria do produto
    p.brand,                                                             -- marca do produto

    -- Margem absoluta: quanto ganhamos por item
    -- Diferença entre o que vendemos e o que custou
    ROUND(oi.sale_price - p.cost, 2)                  AS margem,

    -- Margem percentual: % do preço de venda que é lucro
    -- NULLIF evita erro de divisão por zero
    ROUND(
      (oi.sale_price - p.cost)
      / NULLIF(oi.sale_price, 0) * 100
    , 2)                                              AS margem_pct

  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi

  -- JOIN com products para buscar cost, category e brand
  -- Descoberto na auditoria que cost não existe em order_items
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON oi.product_id = p.id
)

-- SELECT FINAL
SELECT * FROM order_items_filtrados
