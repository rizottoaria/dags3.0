{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Просмотры рекламы (ad_rewards_count) в разрезе версия × страна × бренд девайса.
    Версия/страна/показы — из dim_players; бренд девайса — из amplitude_user_props
    (props.device_type, ключ user_id = player_id). Скоуп версий из var. -#}
{%- set versions = var('report_versions', ['1.0.24','1.0.22']) -%}

with device as (
    -- одна строка на игрока: последний известный device_type
    select
        user_id                                                   as player_id,
        argMax(nullIf(props.device_type::String, ''), updated_at) as device_type
    from {{ source('petbuddy', 'amplitude_user_props') }}
    group by user_id
)

select
    p.app_version,
    ifNull(p.country, '(unknown)')          as country,
    if(d.device_type is null, '(unknown)',
       splitByChar(' ', assumeNotNull(d.device_type))[1]) as device_brand,
    count()                          as players,
    countIf(p.ad_rewards_count > 0)  as ad_viewers,
    sum(p.ad_rewards_count)          as total_ad_rewards
from {{ ref('dim_players') }} p
left join device d on d.player_id = p.player_id
where p.app_version is not null and {{ version_gte('p.app_version', '1.0.22') }}
group by p.app_version, country, device_brand
