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
        toInt32OrNull(properties.chapter::String)           as chapter,
        toFloat64OrNull(properties.revenue::String)         as revenue_amount,
        nullIf(properties.type::String, '')                 as revenue_type
    from {{ source('petbuddy', 'events') }} final
),

agg as (
    select
        player_id,

        min(event_at)                                          as first_seen_at,
        max(event_at)                                          as last_seen_at,
        min(event_date)                                        as first_seen_date,
        max(event_date)                                        as last_seen_date,
        dateDiff('day', min(event_date), max(event_date))      as lifespan_days,
        uniq(event_date)                                       as active_days,

        -- Последние ИЗВЕСТНЫЕ (ненулевые) атрибуты: последнее событие часто ping/pause
        -- с пустыми properties, поэтому обычный argMax дал бы NULL.
        argMaxIf(country, event_at, country is not null)          as country,
        argMaxIf(app_version, event_at, app_version is not null)  as app_version,
        argMaxIf(ab_version, event_at, ab_version is not null)    as ab_version,
        argMaxIf(chapter, event_at, chapter is not null)          as current_chapter,

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
)

select
    player_id,
    first_seen_at,
    last_seen_at,
    first_seen_date,
    last_seen_date,
    lifespan_days,
    active_days,

    country,
    -- ISO-код -> название страны (fallback на сам код для новых стран)
    transform(
        country,
        ['PH','GB','US','AU','SG','ID','BR','HK','FR','IN','UA','TR','DE','RU','VN','CN','CA','ZA','AE','AT','DZ','JO','IR','RO','EG','HU','CZ','TH','AL','PL','IQ','KH','MA','CO','KR','JP','ES','SA','CR','BG','GR','IT','MD','MM','NZ','EC','LY','MN','TT','IE','MX','NI','AZ','FI'],
        ['Philippines','United Kingdom','United States','Australia','Singapore','Indonesia','Brazil','Hong Kong','France','India','Ukraine','Turkey','Germany','Russia','Vietnam','China','Canada','South Africa','United Arab Emirates','Austria','Algeria','Jordan','Iran','Romania','Egypt','Hungary','Czechia','Thailand','Albania','Poland','Iraq','Cambodia','Morocco','Colombia','South Korea','Japan','Spain','Saudi Arabia','Costa Rica','Bulgaria','Greece','Italy','Moldova','Myanmar','New Zealand','Ecuador','Libya','Mongolia','Trinidad and Tobago','Ireland','Mexico','Nicaragua','Azerbaijan','Finland'],
        country
    )                                                      as country_name,

    app_version,
    ab_version,
    current_chapter,

    total_events,
    total_sessions,
    ltv,
    purchases_count,
    ad_rewards_count,
    is_payer
from agg
