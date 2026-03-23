-- CRIAÇÃO DA CAMADA SILVER
-- TABELA: stg_orders
-- OBJETIVO: Filtrar apenas pedidos válidos
--           (Complete e Shipped) e calcular
--           métricas de tempo de entrega
-- FONTE: bigquery-public-data.thelook_ecommerce.orders
-- DESTINO: crm_analytics.stg_orders
-- DECISÃO DE NEGÓCIO: Cancelados e Retornados
--           são excluídos da receita válida
--           mas preservados para análise de perdas
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-15



CREATE OR REPLACE TABLE `integracao-de-dados-the-look.crm_staging.stg_orders` AS

-- CTE 1: orders_filtrados
-- OBJETIVO: Selecionar apenas pedidos com status
--           válido para análise de receita
-- REGRA DE NEGÓCIO:
--   Complete  → pedido entregue e finalizado ok
--   Shipped   → pedido enviado, alta chance de entrega ok
--   Processing → ainda pode ser cancelado 
--   Cancelled  → nunca gerou receita 
--   Returned   → receita estornada 

WITH orders_filtrados AS (
  SELECT
    order_id,                                          -- chave primária do pedido
    user_id,                                           -- chave estrangeira para users
    status,                                            -- status do pedido
    created_at,                                        -- quando o pedido foi criado
    shipped_at,                                        -- quando o pedido foi enviado
    delivered_at,                                      -- quando o pedido foi entregue
    num_of_item,                                       -- quantidade de itens no pedido

    -- Métrica de performance operacional
    -- Mede quantos dias levou entre a criação e o envio
    -- NULL quando shipped_at é nulo (pedidos ainda não enviados)
    DATE_DIFF(shipped_at, created_at, DAY)  AS dias_para_envio

  FROM `bigquery-public-data.thelook_ecommerce.orders`
)

-- SELECT FINAL
SELECT * FROM orders_filtrados
