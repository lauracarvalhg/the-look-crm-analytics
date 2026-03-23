-- CRIAÇÃO DA CAMADA SILVER
-- TABELA: stg_users
-- OBJETIVO: Limpar e deduplicar cadastros de
--           clientes removendo emails duplicados
--           e padronizando campos de texto
-- FONTE: bigquery-public-data.thelook_ecommerce.users
-- DESTINO: crm_analytics.stg_users
-- AUTOR: Laura Carvalho
-- DATA: 2026-03-15


CREATE OR REPLACE TABLE `integracao-de-dados-the-look.crm_staging.stg_users` AS

-- CTE 1: customers_deduplicados
-- OBJETIVO: Numerar cada ocorrência do mesmo email
-- O cadastro mais antigo recebe rn = 1
-- ROW_NUMBER() reinicia a contagem para cada grupo
-- de email igual (PARTITION BY)
-- ORDER BY created_at ASC garante que o mais antigo
-- sempre recebe o número 1

WITH customers_deduplicados AS (
  SELECT
    id,
    first_name,
    last_name,
    email,
    age,
    gender,
    country,
    city,
    traffic_source,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(TRIM(email))  -- agrupa emails iguais ignorando case e espaços
      ORDER BY created_at ASC          -- o cadastro mais antigo recebe rn = 1
    ) AS rn

  FROM `bigquery-public-data.thelook_ecommerce.users`
),

-- CTE 2: customers_limpos
-- OBJETIVO: Aplicar padronização de texto e
--           manter apenas 1 registro por email
-- WHERE rn = 1 elimina todos os duplicados
-- mantendo somente o cadastro mais antigo
customers_limpos AS (
  SELECT
    id                            AS customer_id,     -- renomeia para clareza
    INITCAP(TRIM(first_name))     AS first_name,      -- padroniza capitalização e remove espaços
    INITCAP(TRIM(last_name))      AS last_name,       -- padroniza capitalização e remove espaços
    LOWER(TRIM(email))            AS email,           -- tudo minúsculo e sem espaços
    age,                                              -- sem transformação — sem problemas na auditoria
    gender,                                           -- sem transformação — sem problemas na auditoria
    INITCAP(TRIM(country))        AS country,         -- padroniza capitalização e remove espaços
    INITCAP(TRIM(city))           AS city,            -- padroniza capitalização e remove espaços
    traffic_source,                                   -- sem transformação — canal de aquisição
    created_at                    AS customer_since   -- renomeia para clareza de negócio

  FROM customers_deduplicados
  WHERE rn = 1  -- mantém apenas o cadastro mais antigo de cada email
)

-- SELECT FINAL

SELECT * FROM customers_limpos
