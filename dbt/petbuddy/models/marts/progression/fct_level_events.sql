{{ config(
    materialized='table',
    order_by='(event_date, chapter_key, player_id, event_at)',
    partition_by='toYYYYMM(event_date)',
    tags=['marts','progression','bi'],
    query_settings={
        'max_threads': 1,
        'do_not_merge_across_partitions_select_final': 1
    }
) }}
{#- События прохождения уровней (глав) в едином виде level_start / level_complete / level_fail.
    Отдельных событий level_* игра не шлёт, маппинг на реальные события:
      level_start    <- story_start (вход в главу, тратит энергию);
      level_complete <- chapter_fight_summary IsWin=True  (версии >= 1.0.28),
                        chapter_finish                    (версии <  1.0.28, summary ещё не было);
      level_fail     <- chapter_fight_summary IsWin=False (только >= 1.0.28).
    До 1.0.28 поражения не логировались (has_fail_tracking = false) — fail rate считать
    только по версиям с трекингом. Версии >= 1.0.22 (макрос version_gte).
    Устройство — снимок аккаунта из ben.User.deviceInfo (последний вход, НЕ на момент
    события; заполнено примерно с августа 2026), страна/версия — из самого события. -#}

with ev as (
    select
        id                                                        as event_id,
        event_at,
        toDate(event_at)                                          as event_date,
        name                                                      as source_event,
        player_id,
        toInt32OrNull(properties.chapter::String)                 as chapter,
        nullIf(properties.version::String, '')                    as app_version,
        nullIf(properties.abVersion::String, '')                  as ab_version,
        nullIf(properties.country::String, '')                    as country,
        nullIf(properties.locale::String, '')                     as locale,
        toInt32OrNull(properties.daysSinceRegistration::String)   as days_since_registration,
        toInt32OrNull(properties.allChapterTries::String)         as all_chapter_tries,
        lower(coalesce(nullIf(properties.isStarMode::String, ''),
                       nullIf(properties.IsStarMode::String, ''))) = 'true' as is_star_mode,
        lower(properties.IsWin::String)                           as is_win_raw,
        nullIf(properties.runId::String, '')                      as run_id,
        toInt32OrNull(properties.chapterRunNumber::String)        as chapter_run_number,
        toInt32OrNull(properties.BattlesCount::String)            as battles_count,
        toFloat64OrNull(properties.AvgRounds::String)             as avg_rounds,
        toFloat64OrNull(properties.AvgHpPercent::String)          as avg_hp_percent,
        toInt32OrNull(properties.energy_spend::String)            as energy_spend,
        toFloat64OrNull(properties.playerPower::String)           as player_power
    from {{ source('petbuddy', 'events') }} final
    where name in ('story_start', 'chapter_fight_summary', 'chapter_finish')
      and properties.isTest::String != 'true'
),

typed as (
    select
        *,
        {{ version_gte("ifNull(app_version, '')", '1.0.28') }}  as has_fail_tracking,
        multiIf(
            source_event = 'story_start', 'level_start',
            source_event = 'chapter_fight_summary' and is_win_raw = 'true',  'level_complete',
            source_event = 'chapter_fight_summary' and is_win_raw = 'false', 'level_fail',
            source_event = 'chapter_finish' and not has_fail_tracking,       'level_complete',
            null)                                                 as event_type
    from ev
    where {{ version_gte("ifNull(app_version, '')", '1.0.22') }}
)

select
    t.event_id,
    t.event_at,
    t.event_date,
    t.event_type,
    t.source_event,
    t.player_id,
    t.chapter,
    ifNull(t.chapter, -1)                                         as chapter_key,
    t.is_star_mode,
    t.run_id,
    t.chapter_run_number,
    t.all_chapter_tries,
    t.battles_count,
    t.avg_rounds,
    t.avg_hp_percent,
    t.energy_spend,
    t.player_power,
    t.has_fail_tracking,
    t.days_since_registration,
    {{ lifetime_bucket('t.days_since_registration') }}           as lifetime_bucket,
    {{ lifetime_order('t.days_since_registration') }}            as lifetime_order,
    ifNull(t.app_version, '(unknown)')                            as app_version,
    t.ab_version,
    ifNull(t.country, ifNull(pc.install_country, '(unknown)'))    as country,
    t.locale,
    ifNull(pc.platform, '(unknown)')                              as platform,
    ifNull(pc.device_brand, '(unknown)')                          as device_brand,
    ifNull(pc.device_model, '(unknown)')                          as device_model,
    ifNull(pc.os_name, '(unknown)')                               as os_name,
    pc.os_version                                                 as os_version,
    pc.os_api_level                                               as os_api_level,
    ifNull(pc.is_test_profile, false)                             as is_test_profile
from typed t
left join {{ ref('int_player_campaign') }} pc using (player_id)
where t.event_type is not null
