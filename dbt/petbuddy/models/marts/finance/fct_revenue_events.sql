{{ config(materialized='table', order_by='(event_date, player_id)') }}

-- Факт-таблица платёжных событий (одна строка = один платёж).
select
    event_id,
    player_id,
    session_id,
    event_at,
    event_date,
    country,
    app_version,
    ab_version,
    revenue_type,
    revenue_source,
    round(revenue_amount, 6) as revenue_amount
from {{ ref('int_events__revenue') }}

union all

-- Ручной бэкфилл IAP, не попавших в источник событий (seed iap_backfill).
-- Атрибуты когорты (country/app_version/ab_version) берём из dim_players по player_id.
select
    'backfill_' || b.player_id || '_' || toString(b.event_date) as event_id,
    b.player_id,
    ''                                                          as session_id,
    toDateTime(b.event_date)                                    as event_at,
    b.event_date,
    p.country,
    p.app_version,
    p.ab_version,
    'purchase'                                                  as revenue_type,
    'manual_backfill'                                           as revenue_source,
    round(b.revenue_amount, 6)                                  as revenue_amount
from {{ ref('iap_backfill') }} b
left join {{ ref('dim_players') }} p using (player_id)
