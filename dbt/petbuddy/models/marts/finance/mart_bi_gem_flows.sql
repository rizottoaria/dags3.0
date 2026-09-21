{{ config(
    materialized='table',
    order_by='(event_date, direction, gem_source)',
    tags=['marts','finance','bi'],
    query_settings={'max_threads': 1, 'do_not_merge_across_partitions_select_final': 1}
) }}
{#- Движение Gems (Currency_Gems) на грейне события: приход (in) и расход (out).
    Питает Gem Sinks / Gem Sources и их разрез по Player Lifetime на дашбордах.
    Читаем источник напрямую (FINAL) через дешёвые суб-колонки properties.* — без
    toString(properties), чтобы не упираться в лимит памяти CH. Одна строка =
    одно событие экономики, затронувшее Gems (delta != 0). -#}

with ev as (
    select
        player_id,
        toDate(event_at)                                        as event_date,
        toInt64OrNull(properties.Currency_Gems::String)         as gem_delta,
        toInt32OrNull(properties.daysSinceRegistration::String) as dsr,
        nullIf(properties.version::String, '')                  as app_version,
        nullIf(properties.country::String, '')                  as country,
        nullIf(properties.source::String, '')                   as raw_source
    from {{ source('petbuddy', 'events') }} final
    where name in ('resource_top_up', 'resource_consume')
      and properties.Currency_Gems::String != ''
)

select
    e.event_date,
    e.player_id,
    ifNull(pc.campaign_name, '(unknown)')          as marketing_campaign,
    ifNull(e.country, '(unknown)')                 as country,
    ifNull(e.app_version, '(unknown)')             as app_version,
    {{ lifetime_bucket('e.dsr') }}                 as lifetime_bucket,
    {{ lifetime_order('e.dsr') }}                  as lifetime_order,
    if(e.gem_delta >= 0, 'in', 'out')              as direction,
    ifNull(e.raw_source, '(unknown)')              as gem_source,
    {{ spend_category("ifNull(e.raw_source, '(unknown)')") }} as category,
    abs(e.gem_delta)                               as gems
from ev e
left join {{ ref('int_player_campaign') }} pc using (player_id)
where e.gem_delta is not null and e.gem_delta != 0
