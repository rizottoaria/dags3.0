{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Реклама по placement × версия. Скоуп версий: все >= 1.0.22 (version_gte). -#}
{%- set versions = var('report_versions', ['1.0.24','1.0.22']) -%}

with src as (
    select app_version,
           ifNull(revenue_source, '(unknown)') as placement,
           player_id, revenue_amount
    from {{ ref('fct_revenue_events') }}
    where revenue_type = 'ad_reward'
      and app_version is not null and {{ version_gte('app_version', '1.0.22') }}
),
agg as (
    select app_version, placement,
           toUInt32(count())              as ad_impressions,
           toUInt32(uniqExact(player_id)) as unique_viewers,
           sum(revenue_amount)            as ad_revenue
    from src group by app_version, placement
),
ver as (select app_version, sum(ad_revenue) as v from agg group by app_version)
select
    a.app_version,
    a.placement,
    a.ad_impressions,
    a.unique_viewers,
    a.ad_revenue,
    a.ad_revenue / a.ad_impressions * 1000 as ecpm,
    a.ad_revenue / a.unique_viewers        as rev_per_viewer,
    a.ad_revenue / ver.v                   as rev_share_in_version
from agg a
inner join ver on a.app_version = ver.app_version
