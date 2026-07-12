{{ config(
    materialized='table',
    order_by='(player_id)',
    query_settings={
        'max_threads': 1,
        'do_not_merge_across_partitions_select_final': 1,
        'max_bytes_before_external_group_by': 600000000
    }
) }}

-- Одна строка на игрока: профиль, активность и LTV.
--
-- ВНИМАНИЕ (память): читаем НАПРЯМУЮ из источника, а не из stg_events.
-- stg_events делает SELECT * и тянет целиком JSON-колонки properties/context;
-- на сервере с ~4 ГиБ лимитом ClickHouse это упирается в MEMORY_LIMIT_EXCEEDED.
-- Здесь берём только нужные суб-колонки properties.* (колоночное чтение JSON-путей
-- дёшево), uniq вместо uniqExact и partition-local FINAL — так запрос влезает.
with events as (
    select
        player_id,
        event_at,
        toDate(event_at)                                    as event_date,
        name                                                as event_name,
        session_id,
        nullIf(properties.country::String, '')              as country,
        nullIf(properties.version::String, '')              as app_version,
        nullIf(properties.abVersion::String, '')            as ab_version,
        toFloat64OrNull(properties.revenue::String)         as revenue_amount,
        nullIf(properties.type::String, '')                 as revenue_type
    from {{ source('petbuddy', 'events') }} final
)

select
    player_id,

    min(event_at)                                          as first_seen_at,
    max(event_at)                                          as last_seen_at,
    min(event_date)                                        as first_seen_date,
    max(event_date)                                        as last_seen_date,
    dateDiff('day', min(event_date), max(event_date))      as lifespan_days,
    uniq(event_date)                                       as active_days,

    -- Последние известные атрибуты
    argMax(country, event_at)                              as country,
    argMax(app_version, event_at)                          as app_version,
    argMax(ab_version, event_at)                           as ab_version,

    -- Активность
    count()                                                as total_events,
    uniq(session_id)                                       as total_sessions,

    -- Монетизация (LTV)
    round(sumIf(revenue_amount, event_name = 'revenue'), 4)          as ltv,
    countIf(event_name = 'revenue' and revenue_type = 'purchase')    as purchases_count,
    countIf(event_name = 'revenue' and revenue_type = 'ad_reward')   as ad_rewards_count,
    max(event_name = 'revenue' and revenue_type = 'purchase')        as is_payer
from events
group by player_id
