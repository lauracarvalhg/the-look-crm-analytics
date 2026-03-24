-- OBJETIVO: Analisar onde estamos perdendo receita
--           comparando receita bruta vs receita real
--           e identificando o impacto de cancelamentos
--           e devoluções por status, país e canal
-- FONTE: crm_analytics.vw_sales_analysis
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-23
 

-- BLOCO 1: Visão geral — receita bruta vs real
-- Responde: "Qual o tamanho da perda total?"

SELECT
  'Receita Bruta'                               AS tipo,
  COUNT(DISTINCT order_id)                      AS total_pedidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_total
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
 
UNION ALL
 
SELECT
  'Receita Real (Complete + Shipped)'           AS tipo,
  COUNT(DISTINCT order_id)                      AS total_pedidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_total
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido IN ('Complete', 'Shipped')
 
UNION ALL
 
SELECT
  'Receita Perdida (Cancelled + Returned)'      AS tipo,
  COUNT(DISTINCT order_id)                      AS total_pedidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_total
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido IN ('Cancelled', 'Returned')
 
UNION ALL
 
SELECT
  'Receita em Risco (Processing)'               AS tipo,
  COUNT(DISTINCT order_id)                      AS total_pedidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_total
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido = 'Processing';
 
 
-- BLOCO 2: Perda por status — detalhamento
-- Responde: "Cancelamos mais ou devolvemos mais?"

SELECT
  status_pedido,
  COUNT(DISTINCT order_id)                      AS total_pedidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_perdida,
  ROUND(
    COUNT(DISTINCT order_id) * 100.0
    / SUM(COUNT(DISTINCT order_id)) OVER ()
  , 2)                                          AS pct_pedidos
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido IN ('Cancelled', 'Returned')
GROUP BY status_pedido
ORDER BY receita_perdida DESC;
 
 

-- BLOCO 3: Perda por país — top 10
-- Responde: "Em quais mercados perdemos mais?"

SELECT
  country,
  COUNT(DISTINCT order_id)                      AS total_pedidos_perdidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_perdida,
  ROUND(
    SUM(receita_pedido) * 100.0
    / SUM(SUM(receita_pedido)) OVER ()
  , 2)                                          AS pct_receita_perdida
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido IN ('Cancelled', 'Returned')
GROUP BY country
ORDER BY receita_perdida DESC
LIMIT 10;
 
 

-- BLOCO 4: Perda por canal de aquisição
-- Responde: "Qual canal gera mais cancelamentos?"

SELECT
  traffic_source                                AS canal,
  COUNT(DISTINCT order_id)                      AS total_pedidos_perdidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_perdida,
  ROUND(
    COUNT(DISTINCT order_id) * 100.0
    / SUM(COUNT(DISTINCT order_id)) OVER ()
  , 2)                                          AS pct_pedidos_perdidos
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido IN ('Cancelled', 'Returned')
GROUP BY canal
ORDER BY receita_perdida DESC;
 
 

-- BLOCO 5: Evolução mensal da perda
-- Responde: "A perda está aumentando ou diminuindo?"

SELECT
  ano_mes,
  COUNT(DISTINCT order_id)                      AS pedidos_perdidos,
  ROUND(SUM(receita_pedido), 2)                 AS receita_perdida,
  ROUND(
    SUM(receita_pedido) * 100.0
    / SUM(SUM(receita_pedido)) OVER (PARTITION BY EXTRACT(YEAR FROM data_pedido))
  , 2)                                          AS pct_perda_no_ano
FROM `integracao-de-dados-the-look.crm_analytics.vw_sales_analysis`
WHERE status_pedido IN ('Cancelled', 'Returned')
GROUP BY ano_mes, data_pedido
ORDER BY ano_mes DESC
LIMIT 24;
