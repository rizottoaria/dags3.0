{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Просмотры рекламы (ad_rewards_count) в разрезе версия × страна × бренд девайса.
    Источник — dim_players (версия = текущая версия игрока). Скоуп версий из var. -#}
{%- set versions = var('report_versions', ['1.0.24','1.0.22']) -%}

select
    app_version,
    ifNull(country, '(unknown)') as country,
    if(amp_device_type is null, '(unknown)',
       splitByChar(' ', assumeNotNull(amp_device_type))[1]) as device_brand,
    count()                       as players,
    countIf(ad_rewards_count > 0) as ad_viewers,
    sum(ad_rewards_count)         as total_ad_rewards
from {{ ref('dim_players') }}
where app_version in ({{ "'" ~ versions | join("','") ~ "'" }})
group by app_version, country, device_brand
