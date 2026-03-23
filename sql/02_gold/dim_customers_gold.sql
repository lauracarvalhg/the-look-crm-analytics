-- TABELA: dim_customers_gold
-- OBJETIVO: Construir visão 360° de cada cliente
--           unindo dados demográficos limpos com
--           segmentação RFM, métricas de valor
--           e status de ciclo de vida
-- FONTE: crm_analytics.stg_users
--        crm_analytics.stg_order_items
-- DESTINO: crm_analytics.dim_customers_gold
-- NOTA: Clientes sem nenhuma compra são preservados
--       via LEFT JOIN e recebem status 'Sem Compra'
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-26



CREATE OR REPLACE TABLE `integracao-de-dados-the-look.crm_analytics.dim_customers_gold` AS

-- CTE 1: metricas_cliente
-- OBJETIVO: Agregar todo o histórico de compras
--           por cliente em uma única linha
-- GRÃO: 1 linha = 1 cliente com seu resumo completo
-- FONTE: stg_order_items — já filtrada com pedidos
--        válidos (Complete e Shipped)

WITH metricas_cliente AS (
  -- Agrega comportamento de compra por cliente
  -- SEM filtro de status — comportamento real
  -- LTV calculado separadamente com filtro
  SELECT
    oi.user_id,

    -- Comportamento — todos os pedidos
    COUNT(DISTINCT oi.order_id)     AS total_pedidos,
    COUNT(oi.order_item_id)         AS total_itens,

    -- LTV — apenas receita real
    ROUND(SUM(
      CASE WHEN oi.status IN ('Complete','Shipped')
        THEN oi.sale_price ELSE 0 END
    ), 2)                           AS ltv,

    -- Ticket médio — apenas receita real
    ROUND(AVG(
      CASE WHEN oi.status IN ('Complete','Shipped')
        THEN oi.sale_price ELSE NULL END
    ), 2)                           AS ticket_medio,

    -- Linha do tempo — todos os pedidos
    MIN(oi.created_at)              AS primeira_compra,
    MAX(oi.created_at)              AS ultima_compra

  FROM `integracao-de-dados-the-look.crm_staging.stg_order_items` oi
  GROUP BY oi.user_id
),

-- CTE 2: rfm_scores
-- OBJETIVO: Calcular recência em dias e atribuir
--           notas de 1 a 3 para cada dimensão RFM
-- ESCALA DE NOTAS:
--   3 = melhor comportamento
--   1 = pior comportamento
-- RECÊNCIA: quanto menor o número de dias, melhor
-- FREQUÊNCIA: quanto mais pedidos, melhor
-- MONETÁRIO: quanto maior o LTV, melhor
rfm_scores AS (
  -- Calcula recência e atribui notas R, F, M
  -- Recência baseada no último pedido
  -- independente de status
  SELECT
    user_id,
    total_pedidos,
    total_itens,
    ltv,
    ticket_medio,
    primeira_compra,
    ultima_compra,

    -- Recência: dias desde o último pedido
    -- qualquer status — comportamento real
    DATE_DIFF(CURRENT_DATE(), DATE(ultima_compra), DAY) AS dias_desde_ultima_compra,

    -- SCORE DE RECÊNCIA
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(ultima_compra), DAY) <= 90  THEN 3
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(ultima_compra), DAY) <= 180 THEN 2
      ELSE 1
    END AS r_score,

    -- SCORE DE FREQUÊNCIA
    CASE
      WHEN total_pedidos >= 5 THEN 3
      WHEN total_pedidos >= 2 THEN 2
      ELSE 1
    END AS f_score,

    -- SCORE MONETÁRIO — baseado em LTV real
    CASE
      WHEN ltv >= 500 THEN 3
      WHEN ltv >= 200 THEN 2
      ELSE 1
    END AS m_score

  FROM metricas_cliente
),

-- CTE 3: status_cliente
-- OBJETIVO: Classificar cada cliente em um status
--           de ciclo de vida baseado em recência
--           e histórico de compras
-- REGRAS DE CLASSIFICAÇÃO:
--   Novo       → primeira compra nos últimos 90 dias
--   Ativo      → comprou nos últimos 180 dias
--   Churn      → não compra há mais de 180 dias
-- NOTA: A ordem do CASE importa — do mais
--       específico para o mais geral
status_cliente AS (
  -- Classifica cada cliente por status
  -- baseado em comportamento real
  -- qualquer pedido conta para recência
  SELECT
    user_id,
    total_pedidos,
    total_itens,
    ltv,
    ticket_medio,
    primeira_compra,
    ultima_compra,
    dias_desde_ultima_compra,
    r_score,
    f_score,
    m_score,
    CONCAT(r_score, f_score, m_score) AS rfm_score,
    -- STATUS DE CICLO DE VIDA
    -- Responde: "O cliente ainda está ativo?"
    CASE
      WHEN dias_desde_ultima_compra <= 90
       AND total_pedidos = 1    THEN 'Novo'
      WHEN dias_desde_ultima_compra <= 180 THEN 'Ativo'
      ELSE                           'Churn'
    END AS status_cliente,

    -- SEGMENTO RFM
    -- Responde: "Qual o comportamento de compra?"
    CASE
      WHEN r_score = 3 AND f_score >= 2 AND m_score = 3
        THEN 'Cliente Premium'
      WHEN f_score = 3 AND m_score >= 2
        THEN 'Cliente Frequente'
      WHEN r_score = 3 AND f_score = 1 AND m_score >= 2
        THEN 'Alto Valor Recente'
      WHEN r_score = 1 AND f_score >= 2 AND m_score >= 2
        THEN 'Frequente Inativo'
      WHEN r_score = 1 AND f_score = 1 AND m_score = 1
        THEN 'Inativo Baixo Valor'
      WHEN r_score >= 2 AND f_score >= 2 AND m_score = 1
        THEN 'Frequente Baixo Valor'
      WHEN r_score = 3 AND f_score = 1 AND m_score = 1
        THEN 'Primeira Compra'
      ELSE 'Ocasional'
    END AS segmento_rfm,

    -- SEGMENTO POR VALOR
    -- Responde: "Quanto esse cliente vale financeiramente?"
    -- Cortes baseados nos percentis da base:
    -- p25=$36, p75=$148, elite=$500
    CASE
      WHEN ltv = 0    THEN 'Sem Receita'     -- nunca gerou receita real
      WHEN ltv < 36   THEN 'Baixo Valor'     -- abaixo do percentil 25
      WHEN ltv < 148  THEN 'Médio Valor'     -- entre percentil 25 e 75
      WHEN ltv < 500  THEN 'Alto Valor'      -- acima do percentil 75
      ELSE                 'Cliente Elite'   -- topo da base — acima de $500
    END AS segmento_valor

  FROM rfm_scores
),

-- CTE 4: dim_customers_gold
-- OBJETIVO: Unir dados demográficos limpos da
--           stg_users com métricas e segmentação
--           calculadas nas CTEs anteriores
-- LEFT JOIN: garante que clientes cadastrados mas
--            sem nenhuma compra também aparecem
--            na tabela final com status 'Sem Compra'
-- COALESCE: substitui NULL por 0 nos campos numéricos
--           para clientes sem compra — evita erros
--           de cálculo no BI
dim_customers_gold AS (
  SELECT
    -- IDENTIDADE
    u.customer_id,
    u.first_name,
    u.last_name,
    u.email,
    u.age,
    u.gender,
    u.country,
    u.city,
    u.traffic_source,
    u.customer_since,

    -- MÉTRICAS DE VALOR
    COALESCE(s.total_pedidos, 0)          AS total_pedidos,
    COALESCE(s.total_itens, 0)            AS total_itens,
    COALESCE(s.ltv, 0)                    AS ltv,
    COALESCE(s.ticket_medio, 0)           AS ticket_medio,

    -- LINHA DO TEMPO
    s.primeira_compra,
    s.ultima_compra,
    COALESCE(
      s.dias_desde_ultima_compra,
      DATE_DIFF(CURRENT_DATE(), DATE(u.customer_since), DAY)
    )                                     AS dias_desde_ultima_compra,

    -- SEGMENTAÇÃO RFM
    COALESCE(s.r_score, 0)               AS r_score,
    COALESCE(s.f_score, 0)               AS f_score,
    COALESCE(s.m_score, 0)               AS m_score,
    COALESCE(s.rfm_score, '000')         AS rfm_score,
    COALESCE(s.status_cliente, 'Sem Compra')  AS status_cliente,
    COALESCE(s.segmento_rfm, 'Sem Segmento')  AS segmento_rfm,
    COALESCE(s.segmento_valor, 'Sem Receita')  AS segmento_valor
  FROM `integracao-de-dados-the-look.crm_staging.stg_users` u
  LEFT JOIN status_cliente s
    ON u.customer_id = s.user_id
)

--select final

SELECT * FROM dim_customers_gold
