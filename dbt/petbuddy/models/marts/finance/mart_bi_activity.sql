{{ config(
    materialized='table',
    order_by='(event_date, marketing_campaign)',
    tags=['marts','finance','bi'],
    query_settings={
        'max_threads': 1,
        'do_not_merge_across_partitions_select_final': 1,
        'max_bytes_before_external_group_by': 600000000
    }
) }}
{#- Активность на грейне игрок-день. Одна строка = игрок был активен в этот день.
    Питает DAU/WAU/MAU и служит знаменателем "per active user" метрик, с фильтрами
    campaign/country/version/lifetime. DAU = uniqExact(player_id) за день; WAU/MAU =
    uniqExact за окно 7/30 дней. Источник — напрямую (FINAL), суб-колонки. -#}

with pd as (
    select
        toDate(event_at)                                          as event_date,
        player_id,
        max(toInt32OrNull(properties.daysSinceRegistration::String)) as dsr,
        argMaxIf(nullIf(properties.version::String, ''),
                 event_at, properties.version::String != '')      as app_version,
        argMaxIf(nullIf(properties.country::String, ''),
                 event_at, properties.country::String != '')      as country
    from {{ source('petbuddy', 'events') }} final
    group by event_date, player_id
)

select
    pd.event_date,
    pd.player_id,
    ifNull(dp.marketing_campaign, '(unknown)')                    as marketing_campaign,
    ifNull(pd.country, ifNull(dp.country_name, '(unknown)'))      as country,
    ifNull(pd.app_version, ifNull(dp.app_version, '(unknown)'))   as app_version,
    {{ lifetime_bucket('pd.dsr') }}                               as lifetime_bucket,
    {{ lifetime_order('pd.dsr') }}                                as lifetime_order,
    (pd.event_date = dp.first_seen_date)                          as is_new_install
from pd
left join {{ ref('dim_players') }} dp using (player_id)
