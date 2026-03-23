# The Look — CRM & Revenue Analytics

> Data Warehouse completo no BigQuery para CRM e Vendas — do dado bruto ao insight executivo.

[![BigQuery](https://img.shields.io/badge/BigQuery-Standard_SQL-4285F4?logo=google-cloud)](https://cloud.google.com/bigquery)
[![Looker Studio](https://img.shields.io/badge/Looker_Studio-Dashboard-4285F4?logo=google)](https://lookerstudio.google.com)
[![Status](https://img.shields.io/badge/Status-Concluído-success)]()

---

## Sumário

- [Contexto](#contexto)
- [Problema](#problema)
- [Solução](#solução)
- [Arquitetura](#arquitetura)
- [Modelagem](#modelagem)
- [Como Reproduzir](#como-reproduzir)
- [Entregáveis](#entregáveis)
- [Decisões Técnicas](#decisões-técnicas)
- [Lições Aprendidas](#lições-aprendidas)
- [Próximos Passos](#próximos-passos)

---

## Contexto

A **The Look** é um e-commerce de moda que cresceu exponencialmente nos últimos 2 anos. Com o crescimento, a infraestrutura de dados não acompanhou o negócio:

- Times de Marketing trabalhavam com planilhas exportadas manualmente
- O Financeiro usava outro sistema para calcular receita
- Os números não batiam entre as áreas

---

## Problema

Três dores críticas identificadas pela diretoria:

| Dor | Descrição |
|-----|-----------|
| **Cegueira de Cliente** | Sem diferenciação entre clientes — um cliente que comprou uma vez há 3 anos era tratado igual a um que gasta $500/mês |
| **Dados Sujos** | 16.240 cadastros duplicados, países escritos de formas diferentes, datas em fusos misturados |
| **Métricas Inexistentes** | Sem clareza sobre Churn, LTV ou segmentação — budget de Marketing gasto sem retorno mensurável |

---

## Solução

Construção de uma **Single Source of Truth (SSOT)** no BigQuery seguindo a arquitetura medalhão:

```
Fonte Raw → Silver (limpeza) → Gold (modelagem) → BI (visualização)
```

**Stack utilizada:**
- **Google BigQuery Sandbox** — Data Warehouse (gratuito)
- **Looker Studio** — Dashboard executivo (gratuito)  
- **Power BI** — Modelagem relacional avançada
- **Dataset:** `bigquery-public-data.thelook_ecommerce`

---

## Arquitetura

```
bigquery-public-data.thelook_ecommerce (fonte — somente leitura)
         │
         ▼
┌─────────────────────────────────┐
│   crm_staging (Silver)          │
│   stg_users                     │  ← deduplicação por email
│   stg_orders                    │  ← todos os status preservados
│   stg_order_items               │  ← enriquecido com products
│   stg_user_id_mapping           │  ← resolve ids órfãos
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│   crm_analytics (Gold)          │
│   dim_customers_gold            │  ← visão 360° do cliente
│   fact_sales_performance        │  ← métricas de pedido
│   fact_order_items              │  ← métricas de produto
│   vw_sales_analysis             │  ← ponte para o BI
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│   Business Intelligence         │
│   Looker Studio                 │  ← 3 páginas executivas
│   Power BI                      │  ← modelo relacional nativo
└─────────────────────────────────┘
```

---

## Modelagem

### Modelo Dimensional

```
dim_customers_gold (1 cliente)
        │ 1:N
        ▼
fact_sales_performance (1 pedido) ──── 1:N ──→ fact_order_items (1 item)
        │
        ▼
vw_sales_analysis (join fct + dim + mapeamento)
```

### Descrição das tabelas Gold

| Tabela | Grão | Descrição |
|--------|------|-----------|
| `dim_customers_gold` | 1 linha = 1 cliente | Visão 360°: RFM, LTV, segmentação, status de ciclo de vida |
| `fact_sales_performance` | 1 linha = 1 pedido | Receita, margem, tempo de entrega, séries temporais MoM/YoY |
| `fact_order_items` | 1 linha = 1 item | Performance por produto, categoria e marca |
| `vw_sales_analysis` | 1 linha = 1 pedido | Cruzamento fct + dim para consumo no Looker Studio |

### Segmentações implementadas na dim_customers_gold

**status_cliente** — ciclo de vida temporal:
| Status | Critério |
|--------|----------|
| Novo | Último pedido ≤ 90 dias E total_pedidos = 1 |
| Ativo | Último pedido ≤ 180 dias |
| Churn | Último pedido > 180 dias |
| Sem Compra | Nunca fez pedido |

**segmento_rfm** — comportamento de compra:
| Segmento | Critério RFM |
|----------|-------------|
| Cliente Premium | R=3, F≥2, M=3 |
| Cliente Frequente | F=3, M≥2 |
| Alto Valor Recente | R=3, F=1, M≥2 |
| Frequente Inativo | R=1, F≥2, M≥2 |
| Inativo Baixo Valor | R=1, F=1, M=1 |
| Frequente Baixo Valor | R≥2, F≥2, M=1 |
| Primeira Compra | R=3, F=1, M=1 |
| Ocasional | demais combinações |

**segmento_valor** — valor financeiro (cortes baseados em percentis reais):
| Segmento | LTV |
|----------|-----|
| Cliente Elite | > $500 |
| Alto Valor | $148 – $500 (p75) |
| Médio Valor | $36 – $148 (p25–p75) |
| Baixo Valor | < $36 (p25) |
| Sem Receita | LTV = 0 |

---

## Como Reproduzir

### Pré-requisitos
- Conta Google com acesso ao [BigQuery Sandbox](https://console.cloud.google.com/bigquery) (gratuito)
- Acesso ao dataset público `bigquery-public-data.thelook_ecommerce`

### Passo a passo

**1. Criar os datasets no BigQuery**
```sql
-- No BigQuery Console, criar dois datasets:
-- crm_staging  (região: US)
-- crm_analytics (região: US)
```

**2. Executar as queries na ordem**
```
sql/01_staging/stg_users.sql
sql/01_staging/stg_orders.sql
sql/01_staging/stg_order_items.sql
sql/01_staging/stg_user_id_mapping.sql

sql/02_gold/dim_customers_gold.sql
sql/02_gold/fact_sales_performance.sql
sql/02_gold/fact_order_items.sql

sql/03_analysis/vw_sales_analysis.sql
```

**3. Validar o resultado**
```sql
-- Verificar contagem de cada tabela
SELECT 'stg_users'              AS tabela, COUNT(*) AS linhas FROM `crm_staging.stg_users`
UNION ALL
SELECT 'dim_customers_gold',              COUNT(*) FROM `crm_analytics.dim_customers_gold`
UNION ALL
SELECT 'fact_sales_performance',          COUNT(*) FROM `crm_analytics.fact_sales_performance`
UNION ALL
SELECT 'fact_order_items',                COUNT(*) FROM `crm_analytics.fact_order_items`
UNION ALL
SELECT 'vw_sales_analysis',               COUNT(*) FROM `crm_analytics.vw_sales_analysis`
```

**Resultado esperado:**
| Tabela | Linhas |
|--------|--------|
| stg_users | 83.760 |
| dim_customers_gold | 83.760 |
| fact_sales_performance | 125.117 |
| fact_order_items | 181.566 |
| vw_sales_analysis | 125.117 |

**4. Conectar ao Looker Studio**
- Acesse [lookerstudio.google.com](https://lookerstudio.google.com)
- Conecte as fontes: `vw_sales_analysis` e `dim_customers_gold`
- Importe o template do dashboard (link abaixo)

---

## Entregáveis

### Relatório Diagnóstico — Principais Insights

**Onde estamos perdendo receita:**
```
Receita bruta:           $10.796.173
Receita real:             $5.931.473
Receita perdida (45%):    $4.864.699

Cancelamentos:  18.763 pedidos → $1.629.432
Devoluções:     12.487 pedidos → $1.090.888
```

**Estado da base de clientes:**
```
Churn:      46.388 clientes (55%) — inativos há 811 dias em média
Sem Compra: 16.778 clientes (20%) — nunca compraram
Ativo:      14.230 clientes (17%) — LTV médio $100
Novo:        6.364 clientes  (8%) — janela de 90 dias para converter
```

**Oportunidades ocultas:**
```
Frequente Inativo: 2.778 clientes com LTV $333 — reativar 20% = $185k
Alto Valor Recente: 314 clientes — converter em Premium = $64k potencial
Cadastros não convertidos: 16.778 — converter 10% = $99k sem custo de aquisição
```

**Categorias mais eficientes:**
| Categoria | Margem % |
|-----------|----------|
| Blazers & Jackets | 62.0% |
| Skirts | 59.99% |
| Accessories | 59.98% |
| Suits & Sport Coats | 59.92% |

### Dashboard
- 📊 [Ver dashboard no Looker Studio](https://lookerstudio.google.com/reporting/208be2dc-3d55-4cca-bbb3-5942675f2ee6) 
- 3 páginas: Business Overview / Revenue Leakage / Growth Opportunities

---

## Decisões Técnicas

### Por que dois datasets separados?
A separação entre `crm_staging` e `crm_analytics` garante governança: a camada Silver nunca é exposta diretamente para ferramentas de BI, evitando uso indevido de dados não tratados.

### Por que não filtrar status na Silver?
Filtrar `Complete` e `Shipped` na Silver foi um erro identificado durante o projeto. A Silver limpa e padroniza — filtros de regra de negócio pertencem à Gold ou ao BI. Isso garante flexibilidade para análises futuras sem retrabalho.

### Por que duas tabelas Fato?
- `fact_sales_performance` — grão de pedido → métricas de receita e tempo
- `fact_order_items` — grão de item → métricas de produto e categoria

Misturar os dois grãos em uma única tabela geraria distorção: um pedido com 3 itens seria contado 3 vezes no cálculo de receita.

### Por que a vw_sales_analysis?
O Looker Studio não tem modelo relacional nativo — cada gráfico usa uma fonte. A view entrega o cruzamento de `fact_sales_performance` + `dim_customers_gold` em uma fonte única, eliminando a necessidade de blended data e garantindo que todos os pedidos tenham `country` e `traffic_source` corretos via `users` raw.

### O problema dos ids órfãos
A deduplicação da `stg_users` removeu 16.240 ids duplicados. Porém, 12.984 desses ids haviam feito 20.401 pedidos ($1.752.450 em receita). Sem tratamento, esses pedidos ficariam sem `country`, `canal` e `status_cliente`.

**Solução:** `stg_user_id_mapping` mapeia cada id removido ao seu id canonical, usado na `vw_sales_analysis` para garantir atribuição correta.

---

## Lições Aprendidas

1. **Verificar se ids deduplicados fizeram pedidos** — essa pergunta deveria ter sido feita na fase de auditoria, não descoberta durante a modelagem

2. **Não filtrar status na Silver** — filtros de regra de negócio pertencem à camada de análise

3. **Mapear dimensões de análise antes do SQL** — "quais dimensões o dashboard precisa?" evitaria a criação tardia da `fact_order_items`

4. **Perguntar qual ferramenta de BI vai consumir os dados** — saber sobre a limitação do Looker Studio antes teria levado à criação da `vw_sales_analysis` desde o início

---

## Próximos Passos

- [ ] **Power BI** — conectar BigQuery e explorar modelo relacional com DAX
- [ ] **Snapshot temporal** — histórico de `status_cliente` por mês para análise de Churn ao longo do tempo
- [ ] **Análise de funil** — usar tabela `events` para abandono de carrinho
- [ ] **Automação** — orquestrar transformações com dbt ou Dataform
- [ ] **Testes de qualidade** — implementar assertions para validar grão e PKs automaticamente

---

## Estrutura do Repositório

```
the-look-crm-analytics/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── business_rules.md
├── sql/
│   ├── 01_staging/
│   ├── 02_gold/
│   ├── 03_analysis/
│   └── 04_diagnostic/
└── assets/
    ├── architecture_diagram.svg
    └── dimensional_model.svg
```

---

## Contato

Desenvolvido como projeto de portfólio de Engenharia e Análise de Dados.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Conectar-0A66C2?logo=linkedin)](www.linkedin.com/in/laura-carvalho0) 
[![Medium](https://img.shields.io/badge/Medium-Artigo_completo-000000?logo=medium)]([https://medium.com](https://medium.com/@contatolauracgs/como-construi-uma-single-source-of-truth-no-bigquery-para-resolver-inconsist%C3%AAncias-de-crm-e-vendas-0b9eed9a1735?postPublishedType=repub)) 
