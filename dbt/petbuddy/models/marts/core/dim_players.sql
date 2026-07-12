{{ config(
    materialized='table',
    order_by='(player_id)',
    query_settings={'max_threads': 1, 'max_bytes_before_external_group_by': 1000000000}
) }}

-- Одна строка на игрока: профиль, активность и LTV.
with events as (
    select * from {{ ref('stg_events') }}
)

select
    player_id,

    min(event_at)                                          as first_seen_at,
    max(event_at)                                          as last_seen_at,
    min(event_date)                                        as first_seen_date,
    max(event_date)                                        as last_seen_date,
    dateDiff('day', min(event_date), max(event_date))      as lifespan_days,
    uniqExact(event_date)                                  as active_days,

    -- Последние известные атрибуты
    argMax(country, event_at)                              as country,
    argMax(app_version, event_at)                          as app_version,
    argMax(ab_version, event_at)                           as ab_version,

    -- Активность
    count()                                                as total_events,
    uniqExact(session_id)                                  as total_sessions,

    -- Монетизация (LTV)
    round(sumIf(revenue_amount, event_name = 'revenue'), 4)          as ltv,
    countIf(event_name = 'revenue' and revenue_type = 'purchase')    as purchases_count,
    countIf(event_name = 'revenue' and revenue_type = 'ad_reward')   as ad_rewards_count,
    max(event_name = 'revenue' and revenue_type = 'purchase')        as is_payer
from events
group by player_id
