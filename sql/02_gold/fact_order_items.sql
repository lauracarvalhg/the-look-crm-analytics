-- TABELA: fct_order_items
-- OBJETIVO: Análise de performance por produto
--           categoria e marca
-- GRÃO: 1 linha = 1 item de 1 pedido
-- NOTA: Campos de cliente e pedido estão em
--       outras tabelas Gold — não duplicados aqui
--       Para cruzar: JOIN via order_id ou user_id
-- DECISÃO: 160 itens sem marca (0.08% do total)
--          substituídos por 'Sem Marca'
-- FONTE: stg_order_items
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-26

CREATE OR REPLACE TABLE
`integracao-de-dados-the-look.crm_analytics.fact_order_items` AS

SELECT
  -- IDENTIFICAÇÃO
  -- Chaves para JOIN com outras tabelas Gold
  oi.order_item_id,               -- PK do item
  oi.order_id,                    -- FK → fct_sales_performance
  oi.user_id,                     -- FK → dim_customers_gold

  -- PRODUTO
  oi.category,                    -- categoria do produto
  COALESCE(oi.brand, 'Sem Marca') AS brand,  -- marca do produto

  -- STATUS DO ITEM
  oi.status                       AS status_item,

  -- DATAS
  DATE(oi.created_at)             AS data_pedido,
  EXTRACT(YEAR FROM oi.created_at)  AS ano,
  EXTRACT(MONTH FROM oi.created_at) AS mes,
  FORMAT_DATE('%Y-%m', DATE(oi.created_at)) AS ano_mes,

  -- MÉTRICAS FINANCEIRAS POR ITEM
  -- Grão correto — receita de 1 item específico
  oi.sale_price,                  -- receita do item
  oi.cost,                        -- custo do item
  oi.margem,                      -- margem absoluta do item
  oi.margem_pct                   -- % de margem do item

FROM `integracao-de-dados-the-look.crm_staging.stg_order_items` oi
