{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Когортная выручка по дню закупки в разрезе РЕКЛАМНОЙ КАМПАНИИ — вход прогноза ARPU.
    Грейн: версия установки x страна-сегмент (ALL/US/PH) x кампания-сегмент (ALL / campaign)
    x день закупки x день жизни (плотная сетка 0..min(30, возраст когорты)).
    Сегменты 'ALL' считаются здесь же (arrayJoin), поэтому ALL/ALL совпадает с mart_cohort_daily,
    а <страна>/ALL — с mart_cohort_daily_country. Только выручка (без ретеншена — он тяжёлый
    из-за stg_events и прогнозу не нужен). Кампания = dim_players.marketing_campaign
    ('(unknown)' = нет кампании в ben: органика или потерянная атрибуция). -#}
{%- set countries = var('report_countries', ['US','PH']) -%}

with players as (
    select player_id,
           first_app_version  as cohort_version,
           first_seen_date    as cohort_date,
           first_country,
           marketing_campaign
    from {{ ref('dim_players') }}
    where first_app_version is not null and {{ version_gte('first_app_version', '1.0.22') }}
),
seg as (   -- игрок попадает в сегмент ALL и в свой (страна x кампания) — декартово произведение
    select player_id, cohort_version, cohort_date,
           arrayJoin(if(first_country in ({{ "'" ~ countries | join("','") ~ "'" }}),
                        ['ALL', first_country], ['ALL']))   as country,
           arrayJoin(['ALL', marketing_campaign])           as campaign
    from players
),
player_day as (   -- выручка игрока по дню жизни: реклама (USD) из событий + IAP USD (int_iap_usd)
    select player_id, d, sum(ad) as ad_revenue, sum(iap) as iap_revenue, sum(imp) as ad_impressions
    from (
        select p.player_id as player_id, toInt32(r.event_date - p.cohort_date) as d,
               r.revenue_amount as ad, 0. as iap, toUInt32(1) as imp
        from {{ ref('fct_revenue_events') }} r
        inner join players p on p.player_id = r.player_id
        where r.revenue_type = 'ad_reward'
          and r.event_date >= p.cohort_date and r.event_date - p.cohort_date <= 30
        union all
        select p.player_id, toInt32(t.purchase_date - p.cohort_date), 0., t.usd_amount, toUInt32(0)
        from {{ ref('int_iap_usd') }} t
        inner join players p on p.player_id = t.player_id
        where t.purchase_date between p.cohort_date and p.cohort_date + 30
    )
    group by player_id, d
),
sizes as (
    select cohort_version, country, campaign, cohort_date, count() as cohort_size
    from seg group by cohort_version, country, campaign, cohort_date
),
skeleton as (
    select cohort_version, country, campaign, cohort_date, cohort_size,
           toInt32(arrayJoin(range(0, toUInt32(least(toInt64(30), toInt64(today() - cohort_date))) + 1))) as day_since_install
    from sizes
),
rev as (
    select s.cohort_version as cohort_version, s.country as country, s.campaign as campaign,
           s.cohort_date as cohort_date, pd.d as d,
           sum(pd.ad_revenue) as ad_revenue, sum(pd.iap_revenue) as iap_revenue,
           sum(pd.ad_impressions) as ad_impressions
    from seg s inner join player_day pd on pd.player_id = s.player_id
    group by cohort_version, country, campaign, cohort_date, d
),
base as (
    select sk.cohort_version as cohort_version, sk.country as country, sk.campaign as campaign,
           sk.cohort_date as cohort_date, sk.day_since_install as day_since_install,
           sk.cohort_size as cohort_size,
           ifNull(r.ad_revenue, 0)     as ad_revenue,
           ifNull(r.iap_revenue, 0)    as iap_revenue,
           ifNull(r.ad_impressions, 0) as ad_impressions
    from skeleton sk
    left join rev r
      on sk.cohort_version = r.cohort_version and sk.country = r.country and sk.campaign = r.campaign
     and sk.cohort_date = r.cohort_date and sk.day_since_install = r.d
)
select
    b.cohort_version, b.country, b.campaign, b.cohort_date,
    b.day_since_install,
    b.cohort_size,
    toInt32(today() - b.cohort_date) as cohort_age_days,
    b.ad_revenue, b.ad_impressions, b.iap_revenue,
    b.ad_revenue + b.iap_revenue as total_revenue,
    sum(b.ad_revenue)  over w as cum_ad_revenue,
    sum(b.iap_revenue) over w as cum_iap_revenue,
    sum(b.ad_revenue + b.iap_revenue) over w as cum_total_revenue,
    (sum(b.ad_revenue + b.iap_revenue) over w) / b.cohort_size as arpu_cum
from base b
window w as (partition by b.cohort_version, b.country, b.campaign, b.cohort_date order by b.day_since_install
            rows between unbounded preceding and current row)
