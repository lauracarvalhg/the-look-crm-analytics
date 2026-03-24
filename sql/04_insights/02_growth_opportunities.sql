-- QUERY: 06_growth_opportunities
-- OBJETIVO: Identificar oportunidades ocultas
--           de crescimento de receita sem
--           custo adicional de aquisição
-- FONTE: crm_analytics.dim_customers_gold
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-23

 

-- BLOCO 1: Cadastros sem compra — base não convertida
-- Responde: "Quanto dinheiro não capturamos?"

SELECT
  traffic_source                                AS canal,
  COUNT(*)                                      AS cadastros_sem_compra,
 
  -- Potencial de receita se convertidos
  -- com o LTV médio de clientes Ativos
  ROUND(
    COUNT(*) * (
      SELECT AVG(ltv)
      FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
      WHERE status_cliente = 'Ativo'
    )
  , 2)                                          AS receita_potencial,
 
  -- Potencial conservador: conversão de apenas 10%
  ROUND(
    COUNT(*) * 0.10 * (
      SELECT AVG(ltv)
      FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
      WHERE status_cliente = 'Ativo'
    )
  , 2)                                          AS receita_potencial_10pct
 
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE status_cliente = 'Sem Compra'
GROUP BY canal
ORDER BY cadastros_sem_compra DESC;
 
 

-- BLOCO 2: Frequente Inativo — reativação
-- Responde: "Quanto vale reativar clientes perdidos?"

SELECT
  traffic_source                                AS canal,
  COUNT(*)                                      AS total_frequente_inativo,
  ROUND(AVG(ltv), 2)                            AS ltv_medio_atual,
  ROUND(SUM(ltv), 2)                            AS ltv_total_historico,
  ROUND(AVG(dias_desde_ultima_compra), 0)       AS dias_medio_inativo,
 
  -- Potencial de reativação de 20% do grupo
  ROUND(
    COUNT(*) * 0.20 * AVG(ltv)
  , 2)                                          AS receita_potencial_reativacao_20pct
 
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE segmento_rfm = 'Frequente Inativo'
GROUP BY canal
ORDER BY total_frequente_inativo DESC;
 
 

-- BLOCO 3: Alto Valor Recente — upgrade para Premium
-- Responde: "Quem tem potencial de virar Cliente Premium?"

SELECT
  traffic_source                                AS canal,
  COUNT(*)                                      AS total_alto_valor_recente,
  ROUND(AVG(ltv), 2)                            AS ltv_medio_atual,
 
  -- LTV médio de um Cliente Premium
  ROUND((
    SELECT AVG(ltv)
    FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
    WHERE segmento_rfm = 'Cliente Premium'
  ), 2)                                         AS ltv_medio_premium,
 
  -- Gap: quanto cada cliente pode crescer
  ROUND(
    (SELECT AVG(ltv)
     FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
     WHERE segmento_rfm = 'Cliente Premium')
    - AVG(ltv)
  , 2)                                          AS gap_para_premium,
 
  -- Potencial total se todos virarem Premium
  ROUND(
    COUNT(*) * (
      (SELECT AVG(ltv)
       FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
       WHERE segmento_rfm = 'Cliente Premium')
      - AVG(ltv)
    )
  , 2)                                          AS potencial_total_upgrade
 
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE segmento_rfm = 'Alto Valor Recente'
GROUP BY canal
ORDER BY total_alto_valor_recente DESC;
 
 

-- BLOCO 4: Clientes Novos — janela de conversão
-- Responde: "Temos 90 dias para agir — quantos e onde?"

SELECT
  country                                       AS pais,
  traffic_source                                AS canal,
  COUNT(*)                                      AS total_novos,
  ROUND(AVG(ltv), 2)                            AS ltv_medio_atual,
  ROUND(AVG(dias_desde_ultima_compra), 0)       AS dias_medio_desde_compra,
 
  -- Urgência: dias restantes na janela de 90 dias
  ROUND(90 - AVG(dias_desde_ultima_compra), 0)  AS dias_restantes_janela,
 
  -- Potencial se virarem Ativos
  ROUND(
    COUNT(*) * (
      SELECT AVG(ltv)
      FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
      WHERE status_cliente = 'Ativo'
    )
  , 2)                                          AS potencial_se_ativos
 
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE status_cliente = 'Novo'
GROUP BY pais, canal
HAVING ROUND(90 - AVG(dias_desde_ultima_compra), 0) > 0
ORDER BY total_novos DESC
LIMIT 20;
 
 

-- BLOCO 5: Resumo executivo de oportunidades
-- Responde: "Qual o potencial total de crescimento?"

SELECT
  'Cadastros Sem Compra'                        AS grupo,
  COUNT(*)                                      AS total_clientes,
  0                                             AS ltv_medio_atual,
  ROUND(
    COUNT(*) * 0.10 * (
      SELECT AVG(ltv)
      FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
      WHERE status_cliente = 'Ativo'
    )
  , 2)                                          AS potencial_conservador_10pct
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE status_cliente = 'Sem Compra'
 
UNION ALL
 
SELECT
  'Frequente Inativo — Reativar 20%'            AS grupo,
  COUNT(*)                                      AS total_clientes,
  ROUND(AVG(ltv), 2)                            AS ltv_medio_atual,
  ROUND(COUNT(*) * 0.20 * AVG(ltv), 2)         AS potencial_conservador_10pct
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE segmento_rfm = 'Frequente Inativo'
 
UNION ALL
 
SELECT
  'Alto Valor Recente — Upgrade Premium'        AS grupo,
  COUNT(*)                                      AS total_clientes,
  ROUND(AVG(ltv), 2)                            AS ltv_medio_atual,
  ROUND(COUNT(*) * 0.30 * (
    (SELECT AVG(ltv)
     FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
     WHERE segmento_rfm = 'Cliente Premium')
    - AVG(ltv)
  ), 2)                                         AS potencial_conservador_10pct
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE segmento_rfm = 'Alto Valor Recente'
 
UNION ALL
 
SELECT
  'Clientes Novos — Converter em Ativos'        AS grupo,
  COUNT(*)                                      AS total_clientes,
  ROUND(AVG(ltv), 2)                            AS ltv_medio_atual,
  ROUND(
    COUNT(*) * (
      SELECT AVG(ltv)
      FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
      WHERE status_cliente = 'Ativo'
    )
  , 2)                                          AS potencial_conservador_10pct
FROM `integracao-de-dados-the-look.crm_analytics.dim_customers_gold`
WHERE status_cliente = 'Novo'
 
ORDER BY potencial_conservador_10pct DESC;
 
