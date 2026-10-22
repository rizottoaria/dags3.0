{{ config(
    materialized='view',
    tags=['marts','finance','bi']
) }}
{#- Выручка в USD по дням с разбивкой на источник: Ads (rewarded ads, mart_bi_ads) и
    IAP (покупки, mart_bi_iap). Те же измерения, что у остальных BI-витрин, —
    нативные фильтры дашборда применяются по именам колонок. Питает график
    "Revenue by day — Ads vs IAP". -#}

select
    event_date, 'Ads' as revenue_source,
    marketing_campaign, country, app_version, install_platform, install_date, lifetime_bucket,
    sum(ad_revenue) as revenue_usd
from {{ ref('mart_bi_ads') }}
group by all

union all

select
    event_date, 'IAP' as revenue_source,
    marketing_campaign, country, app_version, install_platform, install_date, lifetime_bucket,
    sum(usd_amount) as revenue_usd
from {{ ref('mart_bi_iap') }}
group by all
