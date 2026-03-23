-- TABELA: fct_sales_performance
-- OBJETIVO: Tabela fato de vendas com métricas
--           de tempo, ticket médio e séries
--           temporais MoM e YoY
--           ATUALIZAÇÃO: enriquecida com dados
--           do cliente via JOIN com dim_customers_gold
--           para permitir análise temporal de
--           status_cliente, churn e canais
-- FONTE: stg_order_items + stg_orders
--        + dim_customers_gold (enriquecimento)
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-15
-- ============================================

CREATE OR REPLACE TABLE `integracao-de-dados-the-look.crm_analytics.fact_sales_performance` AS

WITH vendas_base AS (
  -- PASSO 1: Une itens de pedido com cabeçalho do pedido
  -- Grão: 1 linha = 1 item de 1 pedido
  -- Traz datas, receita, margem e categoria juntos
  SELECT
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    oi.category,
    oi.brand,
    oi.sale_price,
    oi.cost,
    oi.margem,
    oi.margem_pct,
    oi.status,

    -- Datas extraídas para facilitar agregações temporais
    oi.created_at                             AS item_created_at,
    DATE(oi.created_at)                       AS data_pedido,
    EXTRACT(YEAR FROM oi.created_at)          AS ano,
    EXTRACT(MONTH FROM oi.created_at)         AS mes,
    FORMAT_DATE('%Y-%m', DATE(oi.created_at)) AS ano_mes,

    -- Métricas de tempo vindas da stg_orders
    o.dias_para_envio,
    o.shipped_at,
    o.delivered_at

  FROM `integracao-de-dados-the-look.crm_staging.stg_order_items` oi
  LEFT JOIN `integracao-de-dados-the-look.crm_staging.stg_orders` o
    ON oi.order_id = o.order_id
),

metricas_por_pedido AS (
  -- PASSO 2: Agrega itens por pedido
  -- Grão: 1 linha = 1 pedido completo
  -- Calcula ticket médio e métricas financeiras por pedido
  SELECT
    order_id,
    user_id,
    data_pedido,
    ano,
    mes,
    ano_mes,
    status,
    dias_para_envio,
    COUNT(order_item_id)              AS total_itens,
    ROUND(SUM(sale_price), 2)         AS receita_pedido,
    ROUND(SUM(cost), 2)               AS custo_pedido,
    ROUND(SUM(margem), 2)             AS margem_pedido,
    ROUND(AVG(margem_pct), 2)         AS margem_pct_media,
    ROUND(SUM(sale_price) /
      NULLIF(COUNT(order_item_id),0)
    , 2)                              AS ticket_medio_pedido
  FROM vendas_base
  GROUP BY
    order_id, user_id, data_pedido,
    ano, mes, ano_mes, status, dias_para_envio
),

series_temporais AS (
  -- PASSO 3: Agrega por mês para calcular MoM e YoY
  -- FILTRO: apenas Complete e Shipped
  -- Garante que MoM e YoY refletem receita real
  -- Cancelamentos e devoluções excluídos das séries
  SELECT
    ano,
    mes,
    ano_mes,
    COUNT(DISTINCT order_id)          AS total_pedidos,
    COUNT(DISTINCT user_id)           AS clientes_unicos,
    ROUND(SUM(receita_pedido), 2)     AS receita_mensal,
    ROUND(AVG(ticket_medio_pedido),2) AS ticket_medio_mensal,
    ROUND(AVG(dias_para_envio), 1)    AS media_dias_envio,
    ROUND(
      (SUM(receita_pedido) - LAG(SUM(receita_pedido), 1)
        OVER (ORDER BY ano, mes))
      / NULLIF(LAG(SUM(receita_pedido), 1)
        OVER (ORDER BY ano, mes), 0) * 100
    , 2)                              AS crescimento_mom_pct,
    ROUND(
      (SUM(receita_pedido) - LAG(SUM(receita_pedido), 12)
        OVER (ORDER BY ano, mes))
      / NULLIF(LAG(SUM(receita_pedido), 12)
        OVER (ORDER BY ano, mes), 0) * 100
    , 2)                              AS crescimento_yoy_pct
  FROM metricas_por_pedido
  WHERE status IN ('Complete', 'Shipped')  -- ← única mudança
  GROUP BY ano, mes, ano_mes
)

-- RESULTADO FINAL
-- Une todas as camadas em uma tabela fato completa
-- NOVO: JOIN com dim_customers_gold para enriquecer
--       cada pedido com o contexto do cliente
--       Grão mantido: 1 linha = 1 pedido
--       JOIN é N para 1 — não duplica linhas
SELECT
  -- Chave e identificação
  mp.order_id,
  mp.user_id,
  mp.data_pedido,
  mp.ano,
  mp.mes,
  mp.ano_mes,
  mp.status,

  -- Métricas operacionais
  mp.total_itens,
  mp.dias_para_envio,
  st.media_dias_envio,

  -- Métricas financeiras
  mp.receita_pedido,
  mp.custo_pedido,
  mp.margem_pedido,
  mp.margem_pct_media,
  mp.ticket_medio_pedido,

  -- Séries temporais
  st.receita_mensal,
  st.total_pedidos        AS total_pedidos_mes,
  st.clientes_unicos      AS clientes_unicos_mes,
  st.ticket_medio_mensal,
  st.crescimento_mom_pct,
  st.crescimento_yoy_pct,

  -- ENRIQUECIMENTO: contexto do cliente
  -- COALESCE garante que pedidos sem match na dim
  -- recebem 'Sem Compra' em vez de NULL
  -- Elimina a barra vazia no Looker Studio
  COALESCE(d.status_cliente, 'Sem Compra')  AS status_cliente,
  COALESCE(d.rfm_score, '000')              AS rfm_score,
  COALESCE(d.ltv, 0)                        AS ltv,
  COALESCE(d.traffic_source, 'Desconhecido') AS traffic_source,
  COALESCE(d.country, 'Desconhecido')         AS country

FROM metricas_por_pedido mp
LEFT JOIN series_temporais st
  ON mp.ano_mes = st.ano_mes

-- JOIN com dim para trazer contexto do cliente
-- LEFT JOIN garante que pedidos sem match não são perdidos
LEFT JOIN `integracao-de-dados-the-look.crm_analytics.dim_customers_gold` d
  ON mp.user_id = d.customer_id
