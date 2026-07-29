{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Реклама по placement × версия × страна (US, PH). -#}
{%- set versions = var('report_versions', ['1.0.24','1.0.22']) -%}
{%- set countries = var('report_countries', ['US','PH']) -%}

with src as (
    select app_version,
           ifNull(country,'(unknown)')        as country,
           ifNull(revenue_source,'(unknown)') as placement,
           player_id, revenue_amount
    from {{ ref('fct_revenue_events') }}
    where revenue_type = 'ad_reward'
      and app_version in ({{ "'" ~ versions | join("','") ~ "'" }})
      and country in ({{ "'" ~ countries | join("','") ~ "'" }})
),
agg as (
    select app_version, country, placement,
           toUInt32(count())              as ad_impressions,
           toUInt32(uniqExact(player_id)) as unique_viewers,
           sum(revenue_amount)            as ad_revenue
    from src group by app_version, country, placement
),
ver as (select app_version, country, sum(ad_revenue) as v from agg group by app_version, country)
select
    a.app_version, a.country, a.placement, a.ad_impressions, a.unique_viewers, a.ad_revenue,
    a.ad_revenue / a.ad_impressions * 1000 as ecpm,
    a.ad_revenue / a.unique_viewers        as rev_per_viewer,
    a.ad_revenue / ver.v                   as rev_share_in_version_country
from agg a inner join ver on a.app_version = ver.app_version and a.country = ver.country
