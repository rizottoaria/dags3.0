{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Прогноз накопительного ARPU на install (ad и total), D1..D60.
    Наклон роста b — из лог-регрессии cum_arpu ~ a + b*ln(day) по ЧИСТОЙ fixed-cohort кривой
    (окно 1..H, H=least(14,возраст старшей когорты), когорты age>=H → монотонно).
    ЯКОРЕНИЕ: прогноз = observed(anchor=D7) + b*(ln(day)-ln(7)); так кривая непрерывна с фактом
    и опирается на агрегат по ВСЕМ когортам, а не только по старым. Сегменты: версия + версия×страна. -#}
{%- set anchor = 7 -%}

with src as (
    select cohort_version, 'ALL' as country, cohort_age_days, day_since_install, cohort_size, cum_ad_revenue, cum_total_revenue
    from {{ ref('mart_cohort_daily') }}
    union all
    select cohort_version, country, cohort_age_days, day_since_install, cohort_size, cum_ad_revenue, cum_total_revenue
    from {{ ref('mart_cohort_daily_country') }}
),
seg_h as (
    select cohort_version, country, least(toInt32(14), toInt32(max(cohort_age_days))) as H
    from src group by cohort_version, country
),
fit_pts as (
    select s.cohort_version as cohort_version, s.country as country, s.day_since_install as d,
           sum(s.cum_ad_revenue)/sum(s.cohort_size)    as y_ad,
           sum(s.cum_total_revenue)/sum(s.cohort_size) as y_tot
    from src s inner join seg_h h on s.cohort_version=h.cohort_version and s.country=h.country
    where s.cohort_age_days >= h.H and s.day_since_install between 1 and h.H
    group by s.cohort_version, s.country, s.day_since_install
),
reg as (
    select cohort_version, country,
           (simpleLinearRegression(log(d), y_ad)).1  as b_ad,
           (simpleLinearRegression(log(d), y_tot)).1 as b_tot,
           count() as fit_points
    from fit_pts group by cohort_version, country
),
observed as (
    select cohort_version, country, day_since_install as d,
           sum(cum_ad_revenue)/sum(cohort_size)    as obs_ad,
           sum(cum_total_revenue)/sum(cohort_size) as obs_tot
    from src group by cohort_version, country, day_since_install
),
maxobs as (
    select cohort_version, country, least(toInt32(30), toInt32(max(cohort_age_days))) as last_obs_day
    from src group by cohort_version, country
),
anchor as (
    select cohort_version, country, obs_ad as anch_ad, obs_tot as anch_tot
    from observed where d = {{ anchor }}
),
days as ( select toInt32(arrayJoin(range(1,61))) as d )
select
    r.cohort_version as cohort_version,
    r.country        as country,
    d.d              as day_since_install,
    r.fit_points,
    o.obs_ad  as observed_cum_ad_arpu,
    o.obs_tot as observed_cum_arpu,
    greatest(0, a.anch_ad  + r.b_ad  * (log(d.d) - log({{ anchor }}))) as forecast_cum_ad_arpu,
    greatest(0, a.anch_tot + r.b_tot * (log(d.d) - log({{ anchor }}))) as forecast_cum_arpu,
    multiIf(d.d > m.last_obs_day, greatest(0, a.anch_ad  + r.b_ad  * (log(d.d) - log({{ anchor }}))), o.obs_ad)  as best_cum_ad_arpu,
    multiIf(d.d > m.last_obs_day, greatest(0, a.anch_tot + r.b_tot * (log(d.d) - log({{ anchor }}))), o.obs_tot) as best_cum_arpu,
    p.prophet_cum_ad_arpu as prophet_cum_ad_arpu,
    p.prophet_cum_arpu as prophet_cum_arpu,
    toUInt8(d.d > m.last_obs_day) as is_forecast
from reg r
cross join days d
inner join maxobs m on m.cohort_version=r.cohort_version and m.country=r.country
inner join anchor a on a.cohort_version=r.cohort_version and a.country=r.country
left join observed o on o.cohort_version=r.cohort_version and o.country=r.country and o.d=d.d
left join petbuddy_clean.arpu_prophet_raw p on p.cohort_version=r.cohort_version and p.country=r.country and p.day_since_install=d.d
