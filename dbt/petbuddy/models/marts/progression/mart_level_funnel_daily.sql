{{ config(
    materialized='table',
    order_by='(event_date, chapter_key, app_version, country, os_name, device_brand)',
    tags=['marts','progression','bi']
) }}
{#- Дневная воронка прохождения глав: день x глава x режим x версия x страна x платформа/ОС/бренд.
    starts/completes/fails — события, *_players — уникальные игроки в срезе (между срезами
    не суммируются). abandons = старты без исхода (выход/закрытие игры) — осмысленны
    только при has_fail_tracking (версии >= 1.0.28). -#}

select
    event_date,
    chapter,
    chapter_key,
    is_star_mode,
    app_version,
    has_fail_tracking,
    country,
    platform,
    os_name,
    device_brand,
    lifetime_bucket,
    any(lifetime_order)                                           as lifetime_order,
    countIf(event_type = 'level_start')                           as starts,
    countIf(event_type = 'level_complete')                        as completes,
    countIf(event_type = 'level_fail')                            as fails,
    if(has_fail_tracking, greatest(starts - completes - fails, 0), null) as abandons,
    uniqExactIf(player_id, event_type = 'level_start')            as start_players,
    uniqExactIf(player_id, event_type = 'level_complete')         as complete_players,
    uniqExactIf(player_id, event_type = 'level_fail')             as fail_players,
    if(starts > 0, round(completes / starts, 4), null)            as complete_rate,
    if(has_fail_tracking and completes + fails > 0,
       round(fails / (completes + fails), 4), null)               as fail_rate,
    avgIf(battles_count, event_type in ('level_complete', 'level_fail')) as avg_battles,
    avgIf(avg_hp_percent, event_type = 'level_fail')              as avg_hp_percent_on_fail
from {{ ref('fct_level_events') }}
where not is_test_profile
group by event_date, chapter, chapter_key, is_star_mode, app_version, has_fail_tracking,
         country, platform, os_name, device_brand, lifetime_bucket
