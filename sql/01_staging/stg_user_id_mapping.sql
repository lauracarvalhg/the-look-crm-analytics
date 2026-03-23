-- TABELA: stg_user_id_mapping
-- OBJETIVO: Mapear ids duplicados removidos
--           para seus ids canonicos
-- PROBLEMA QUE RESOLVE: 12.984 user_ids existem
--           na fct mas não na dim porque foram
--           removidos pela deduplicação da stg_users
--           Seus pedidos ficaram órfãos sem country,
--           canal e status_cliente corretos
-- FONTE: bigquery-public-data.thelook_ecommerce.users
-- DESTINO: crm_analytics.stg_user_id_mapping
-- AUTOR: Laura Carvalh0
-- DATA: 2026-03-26


CREATE OR REPLACE TABLE `integracao-de-dados-the-look.crm_staging.stg_user_id_mapping` AS

WITH todos_usuarios AS (
  -- Numera todos os cadastros agrupando por email
  -- Mesma lógica da deduplicação da stg_users
  -- O mais antigo (rn=1) é o canonical
  SELECT
    id,
    LOWER(TRIM(email))  AS email,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(TRIM(email))
      ORDER BY created_at ASC  -- mais antigo = canonical
    ) AS rn
  FROM `bigquery-public-data.thelook_ecommerce.users`
),

canonical AS (
  -- Isola apenas o id canonical de cada email
  -- canonical = o cadastro mais antigo que ficou na dim
  SELECT
    email,
    id AS canonical_id
  FROM todos_usuarios
  WHERE rn = 1
)

-- Gera o mapeamento:
-- para cada id duplicado → qual é o canonical
-- Apenas ids removidos (rn > 1) entram aqui
SELECT
  t.id                AS user_id_original,   -- id que foi removido da dim
  c.canonical_id      AS user_id_canonical   -- id que ficou na dim
FROM todos_usuarios t
JOIN canonical c
  ON t.email = c.email
WHERE t.rn > 1  -- apenas duplicatas removidas

