{{ config(materialized='table', tags=['marts','finance','bi'],
    pre_hook="ALTER TABLE petbuddy_clean.arpu_prophet_raw ADD COLUMN IF NOT EXISTS campaign String DEFAULT 'ALL'") }}
{#- Прогноз накопительного ARPU на install (ad и total), D1..D60.
    Сегмент = версия установки x страна (ALL/US/PH) x рекламная кампания (ALL / campaign).
    Наклон роста b — из лог-регрессии cum_arpu ~ a + b*ln(day) по ЧИСТОЙ fixed-cohort кривой
    (окно 1..H, H=least(14,возраст старшей когорты), когорты age>=H → монотонно).
    ЯКОРЕНИЕ: прогноз = observed(anchor=D7) + b*(ln(day)-ln(7)); так кривая непрерывна с фактом
    и опирается на агрегат по ВСЕМ когортам, а не только по старым. -#}
{%- set anchor = 7 -%}

{%- set min_installs = var('prophet_min_installs', 20) -%}

with src as (   -- сегменты: версия установки x страна (ALL/US/PH) x кампания (ALL/campaign)
    select cohort_version, country, campaign, cohort_age_days, day_since_install, cohort_size, cum_ad_revenue, cum_total_revenue
    from {{ ref('mart_cohort_daily_campaign') }}
    where (cohort_version, country, campaign) in (   -- мелкие сегменты (< min_installs установок) не прогнозируем
        select cohort_version, country, campaign from {{ ref('mart_cohort_daily_campaign') }}
        where day_since_install = 0
        group by cohort_version, country, campaign
        having sum(cohort_size) >= {{ min_installs }}
    )
),
seg_h as (
    select cohort_version, country, campaign, least(toInt32(14), toInt32(max(cohort_age_days))) as H
    from src group by cohort_version, country, campaign
),
fit_pts as (
    select s.cohort_version as cohort_version, s.country as country, s.campaign as campaign, s.day_since_install as d,
           sum(s.cum_ad_revenue)/sum(s.cohort_size)    as y_ad,
           sum(s.cum_total_revenue)/sum(s.cohort_size) as y_tot
    from src s inner join seg_h h on s.cohort_version=h.cohort_version and s.country=h.country and s.campaign=h.campaign
    where s.cohort_age_days >= h.H and s.day_since_install between 1 and h.H
    group by s.cohort_version, s.country, s.campaign, s.day_since_install
),
reg as (
    select cohort_version, country, campaign,
           (simpleLinearRegression(log(d), y_ad)).1  as b_ad,
           (simpleLinearRegression(log(d), y_tot)).1 as b_tot,
           count() as fit_points
    from fit_pts group by cohort_version, country, campaign
),
observed as (
    select cohort_version, country, campaign, day_since_install as d,
           sum(cum_ad_revenue)/sum(cohort_size)    as obs_ad,
           sum(cum_total_revenue)/sum(cohort_size) as obs_tot
    from src group by cohort_version, country, campaign, day_since_install
),
maxobs as (
    select cohort_version, country, campaign, least(toInt32(30), toInt32(max(cohort_age_days))) as last_obs_day
    from src group by cohort_version, country, campaign
),
anchor as (
    select cohort_version, country, campaign, obs_ad as anch_ad, obs_tot as anch_tot
    from observed where d = {{ anchor }}
),
days as ( select toInt32(arrayJoin(range(1,61))) as d ),

-- Тримминг последнего наблюдаемого дня: если на нём факт резко проседает (>10% ниже
-- предыдущего дня) — это артефакт maturity-adjusted (меняется состав доживших когорт).
-- Такой день на ЛИНИИ факта не показываем (в сводке best/prophet_obs остаётся сырой факт).
obs_win as (
    select cohort_version, country, campaign, d, obs_tot,
           row_number() over (partition by cohort_version, country, campaign order by d desc) as rn
    from observed
),
obs_trim as (
    select cohort_version, country, campaign,
           if(anyIf(obs_tot, rn = 2) > 0
              and anyIf(obs_tot, rn = 1) < anyIf(obs_tot, rn = 2) * 0.90,
              anyIf(d, rn = 1), toInt32(-1)) as trim_day
    from obs_win
    group by cohort_version, country, campaign
),

fc as (
    select
        r.cohort_version as cohort_version,
        r.country        as country,
        r.campaign       as campaign,
        d.d              as day_since_install,
        r.fit_points,
        -- факт для ЛИНИИ: NULL после last_obs_day (чистый обрыв, без падения в ноль) и
        -- NULL на trim_day (резкий провал последнего дня — не показываем)
        if(d.d > m.last_obs_day or d.d = t.trim_day, null, o.obs_ad)  as observed_cum_ad_arpu,
        if(d.d > m.last_obs_day or d.d = t.trim_day, null, o.obs_tot) as observed_cum_arpu,
        greatest(0, a.anch_ad  + r.b_ad  * (log(d.d) - log({{ anchor }}))) as forecast_cum_ad_arpu,
        greatest(0, a.anch_tot + r.b_tot * (log(d.d) - log({{ anchor }}))) as forecast_cum_arpu,
        multiIf(d.d > m.last_obs_day, greatest(0, a.anch_ad  + r.b_ad  * (log(d.d) - log({{ anchor }}))), o.obs_ad)  as best_cum_ad_arpu,
        multiIf(d.d > m.last_obs_day, greatest(0, a.anch_tot + r.b_tot * (log(d.d) - log({{ anchor }}))), o.obs_tot) as best_cum_arpu,
        p.prophet_cum_ad_arpu as prophet_cum_ad_arpu,
        p.prophet_cum_arpu as prophet_cum_arpu,
        -- Prophet, «прижатый» к факту: на наблюдаемых днях СЫРОЙ факт (не тримленный),
        -- чтобы сводка D30 оставалась фактом; на прогнозных днях — Prophet.
        if(d.d > m.last_obs_day, p.prophet_cum_ad_arpu, o.obs_ad)  as prophet_obs_cum_ad_arpu,
        if(d.d > m.last_obs_day, p.prophet_cum_arpu, o.obs_tot)    as prophet_obs_cum_arpu,
        if(d.d > m.last_obs_day, greatest(0, p.prophet_cum_arpu - p.prophet_cum_ad_arpu),
                                 greatest(0, o.obs_tot - o.obs_ad)) as prophet_obs_cum_iap_arpu,
        toUInt8(d.d > m.last_obs_day) as is_forecast
    from reg r
    cross join days d
    inner join maxobs m on m.cohort_version=r.cohort_version and m.country=r.country and m.campaign=r.campaign
    inner join anchor a on a.cohort_version=r.cohort_version and a.country=r.country and a.campaign=r.campaign
    left join observed o on o.cohort_version=r.cohort_version and o.country=r.country and o.campaign=r.campaign and o.d=d.d
    left join obs_trim t on t.cohort_version=r.cohort_version and t.country=r.country and t.campaign=r.campaign
    left join petbuddy_clean.arpu_prophet_raw p on p.cohort_version=r.cohort_version and p.country=r.country and p.campaign=r.campaign and p.day_since_install=d.d
)

select
    *,
    -- IAP = total - ad (для каждого метода). observed_iap наследует тримминг факта.
    if(isNull(observed_cum_arpu), null, greatest(0, observed_cum_arpu - observed_cum_ad_arpu)) as observed_cum_iap_arpu,
    greatest(0, forecast_cum_arpu   - forecast_cum_ad_arpu)  as forecast_cum_iap_arpu,
    greatest(0, best_cum_arpu       - best_cum_ad_arpu)      as best_cum_iap_arpu,
    greatest(0, prophet_cum_arpu    - prophet_cum_ad_arpu)   as prophet_cum_iap_arpu,
    -- Явные алиасы для чартов (Superset): forecast_iap / observed_iap
    if(isNull(observed_cum_arpu), null, greatest(0, observed_cum_arpu - observed_cum_ad_arpu)) as observed_iap,
    greatest(0, best_cum_arpu     - best_cum_ad_arpu)       as forecast_iap
from fc
