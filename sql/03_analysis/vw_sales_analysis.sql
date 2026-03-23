CREATE OR REPLACE VIEW
`integracao-de-dados-the-look.crm_analytics.vw_sales_analysis` AS

WITH

-- PASSO 1: base de pedidos
-- Grão: 1 linha = 1 pedido
-- Fonte: stg_orders — já tem grão correto
pedidos AS (
  SELECT
    order_id,
    user_id,
    status,
    created_at,
    shipped_at,
    delivered_at,
    dias_para_envio,
    DATE(created_at)                        AS data_pedido,
    EXTRACT(YEAR FROM created_at)           AS ano,
    EXTRACT(MONTH FROM created_at)          AS mes,
    FORMAT_DATE('%Y-%m', DATE(created_at))  AS ano_mes
  FROM `integracao-de-dados-the-look.crm_staging.stg_orders`
),

-- PASSO 2: métricas financeiras por pedido
-- Agrega stg_order_items por order_id
-- Grão resultante: 1 linha = 1 pedido 
financeiro AS (
  SELECT
    order_id,
    COUNT(order_item_id)                    AS total_itens,
    ROUND(SUM(sale_price), 2)               AS receita_pedido,
    ROUND(SUM(cost), 2)                     AS custo_pedido,
    ROUND(SUM(margem), 2)                   AS margem_pedido,
    ROUND(AVG(margem_pct), 2)               AS margem_pct_media,
    ROUND(SUM(sale_price) /
      NULLIF(COUNT(order_item_id), 0), 2)   AS ticket_medio_pedido
  FROM `integracao-de-dados-the-look.crm_staging.stg_order_items`
  GROUP BY order_id
),

-- PASSO 3: contexto do cliente via stg_users raw
-- Usa a tabela raw — não a stg deduplicada
-- Tem TODOS os user_ids incluindo órfãos 
-- JOIN por user_id — não por email
-- Não quebra o grão pois user_id é único no raw
contexto_usuario AS (
  SELECT
    id                AS user_id,
    country,
    city,
    age,
    gender,
    traffic_source,
    created_at        AS customer_since
  FROM `bigquery-public-data.thelook_ecommerce.users`
),

-- Garante 1 linha por user_id_original
-- Evita multiplicação de linhas no JOIN
mapeamento AS (
  SELECT DISTINCT
    user_id_original,
    user_id_canonical
  FROM `integracao-de-dados-the-look.crm_staging.stg_user_id_mapping`
),

-- Calcula última compra e total de pedidos
-- para clientes órfãos não encontrados na dim
-- Base: apenas Complete e Shipped
-- Mesma lógica da dim_customers_gold

ultima_compra AS (
  -- Calcula última compra e total de pedidos
  -- para clientes órfãos não encontrados na dim
  -- Base: todos os pedidos — sem filtro de status
  -- Mesma lógica da dim_customers_gold atualizada
  SELECT
    user_id,
    MAX(DATE(created_at))           AS data_ultima_compra,
    COUNT(DISTINCT order_id)        AS total_pedidos
  FROM `integracao-de-dados-the-look.crm_staging.stg_order_items`
  GROUP BY user_id
)

-- SELECT FINAL
-- JOIN de todos os blocos pelo order_id
-- Grão mantido: 1 linha = 1 pedido 
SELECT
  -- IDENTIFICAÇÃO
  p.order_id,
  p.user_id,
  p.data_pedido,
  p.ano,
  p.mes,
  p.ano_mes,
  p.status                                          AS status_pedido,

  -- MÉTRICAS OPERACIONAIS
  p.dias_para_envio,
  p.shipped_at,
  p.delivered_at,

  -- MÉTRICAS FINANCEIRAS
  f.total_itens,
  f.receita_pedido,
  f.custo_pedido,
  f.margem_pedido,
  f.margem_pct_media,
  f.ticket_medio_pedido,

  -- CONTEXTO DO CLIENTE
  u.country,
  u.city,
  u.age,
  u.gender,
  u.traffic_source,
  u.customer_since,

  -- SEGMENTAÇÃO
  -- Prioridade:
  -- 1. Cliente na dim com status real
  -- 2. Órfão → classifica por última compra
  -- 3. Nunca finalizou compra
CASE
  WHEN d.status_cliente IS NOT NULL
   AND d.status_cliente != 'Sem Compra'
    THEN d.status_cliente
  WHEN uc.data_ultima_compra IS NOT NULL
    THEN
      CASE
        WHEN DATE_DIFF(CURRENT_DATE(),
          uc.data_ultima_compra, DAY) <= 90
         AND uc.total_pedidos = 1  -- ← corrigido
          THEN 'Novo'
        WHEN DATE_DIFF(CURRENT_DATE(),
          uc.data_ultima_compra, DAY) <= 180
          THEN 'Ativo'
        ELSE 'Churn'
      END
  ELSE 'Sem Segmento'
END AS status_cliente,

  COALESCE(d.rfm_score, '000')                      AS rfm_score,
  COALESCE(d.ltv, 0)                                AS ltv,
  COALESCE(d.ticket_medio, 0)                       AS ticket_medio_cliente,
  COALESCE(d.total_pedidos, 0)                      AS total_pedidos_cliente

FROM pedidos p
LEFT JOIN financeiro f
  ON p.order_id = f.order_id
LEFT JOIN contexto_usuario u
  ON p.user_id = u.user_id
LEFT JOIN mapeamento m
  ON p.user_id = m.user_id_original
LEFT JOIN `integracao-de-dados-the-look.crm_analytics.dim_customers_gold` d
  ON COALESCE(m.user_id_canonical, p.user_id) = d.customer_id
LEFT JOIN ultima_compra uc
  ON p.user_id = uc.user_id
