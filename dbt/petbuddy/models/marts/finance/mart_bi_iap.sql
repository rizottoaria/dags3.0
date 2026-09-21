{{ config(
    materialized='table',
    order_by='(event_date, marketing_campaign)',
    tags=['marts','finance','bi']
) }}
{#- Покупки (IAP) в USD на грейне транзакции. Источник — int_iap_usd (гибрид:
    monetization_transactions + конверсия старых событий по курсам). Питает Summary.USD,
    Real Money Purchases (агрегатно — SKU в данных нет) и IAP by Player Lifetime.
    Атрибуты игрока (кампания/страна/версия/install) — из dim_players. -#}

with iap as (
    select player_id, purchase_date, usd_amount
    from {{ ref('int_iap_usd') }}
    where usd_amount is not null
),

first_purchase as (
    select player_id, min(purchase_date) as first_purchase_date
    from iap group by player_id
),

pl as (
    select
        player_id,
        marketing_campaign,
        ifNull(country_name, '(unknown)')                  as country,
        ifNull(app_version, '(unknown)')                   as app_version,
        coalesce(install_date, first_seen_date)            as install_day
    from {{ ref('dim_players') }}
)

select
    i.purchase_date                                        as event_date,
    i.player_id,
    ifNull(p.marketing_campaign, '(unknown)')              as marketing_campaign,
    p.country,
    p.app_version,
    {{ lifetime_bucket('greatest(0, dateDiff(\'day\', p.install_day, i.purchase_date))') }} as lifetime_bucket,
    {{ lifetime_order('greatest(0, dateDiff(\'day\', p.install_day, i.purchase_date))') }} as lifetime_order,
    i.usd_amount,
    1                                                      as purchase_cnt,
    (i.purchase_date = f.first_purchase_date)              as is_first_purchase
from iap i
left join pl p using (player_id)
left join first_purchase f using (player_id)
