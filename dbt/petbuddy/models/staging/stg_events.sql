{{
  config(
    materialized='view'
  )
}}

-- Очищенный, дедуплицированный слой событий.
-- FINAL схлопывает дубли ReplacingMergeTree по received_at.
-- Часто используемые поля из properties вынесены в плоские колонки.

with source as (

    select *
    from {{ source('petbuddy', 'events') }} final

)

select
    id,
    name                                                    as event_name,
    player_id,
    session_id,
    client_session_id,
    event_at,
    toDate(event_at)                                        as event_date,
    source                                                  as event_source,

    -- Общие атрибуты из properties
    nullIf(properties.country::String, '')                 as country,
    nullIf(properties.version::String, '')                 as app_version,
    nullIf(properties.abVersion::String, '')               as ab_version,
    nullIf(properties.chapter::String, '')                 as chapter_raw,
    toInt32OrNull(properties.daysSinceRegistration::String) as days_since_registration,
    toInt32OrNull(properties.allChapterTries::String)       as all_chapter_tries,

    -- Атрибуты монетизации (заполнены только у event_name = 'revenue')
    toFloat64OrNull(properties.revenue::String)             as revenue_amount,
    nullIf(properties.type::String, '')                     as revenue_type,
    nullIf(properties.source::String, '')                   as action_source,

    -- Сырые JSON на случай дальнейшего разбора в intermediate-слое
    properties,
    context,

    received_at,
    ingested_at
from source
