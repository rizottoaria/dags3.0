{{ config(
    materialized='table',
    order_by='(event_date, placement)',
    tags=['marts','finance','bi'],
    query_settings={'max_threads': 1, 'do_not_merge_across_partitions_select_final': 1}
) }}
{#- Просмотры Rewarded Ads на грейне события (revenue, type=ad_reward). Одна строка =
    один просмотр рекламы. placement = properties.source (место показа/за что награда).
    Питает раздел Rewarded Ads и Rewarded Ads by Player Lifetime. ad_revenue — доход
    с показа (если пришёл). Источник — напрямую (FINAL), дешёвые суб-колонки. -#}

with ev as (
    select
        player_id,
        toDate(event_at)                                                  as event_date,
        toFloat64OrNull(replaceAll(properties.revenue::String, ',', '.')) as ad_revenue,
        toInt32OrNull(properties.daysSinceRegistration::String)           as dsr,
        nullIf(properties.version::String, '')                            as app_version,
        nullIf(properties.country::String, '')                            as country,
        nullIf(properties.source::String, '')                             as placement
    from {{ source('petbuddy', 'events') }} final
    where name = 'revenue' and properties.type::String = 'ad_reward'
)

select
    e.event_date,
    e.player_id,
    ifNull(pc.campaign_name, '(unknown)')          as marketing_campaign,
    ifNull(e.country, '(unknown)')                 as country,
    ifNull(e.app_version, '(unknown)')             as app_version,
    {{ lifetime_bucket('e.dsr') }}                 as lifetime_bucket,
    {{ lifetime_order('e.dsr') }}                  as lifetime_order,
    ifNull(e.placement, '(unknown)')               as placement,
    1                                              as ad_view,
    ifNull(e.ad_revenue, 0)                        as ad_revenue
from ev e
left join {{ ref('int_player_campaign') }} pc using (player_id)
