{{ config(
    materialized='table',
    order_by='(clan_id, event_at, event_id)',
    tags=['marts','clans','bi']
) }}
{#- Факт клановых рейдовых боёв (clan_raid_fight), одна строка = один бой, с флагами
    технических аномалий. Отдельного события ошибки игра НЕ шлёт, поэтому сбои
    выявляются эвристически по самим данным боя:
      - is_loss_full_hp   — поражение при HpPercent = 100 (игрок не получил урона):
                            выход/обрыв/таймаут посреди боя или несинхрон HP;
      - is_hp_mismatch    — HpPercent расходится со Stats.CURRENT_HP/MAX_HP > 1 п.п.;
      - is_bad_payload    — нет bossTier / HpPercent / IsWin не True/False / пустые Stats;
      - is_duplicate      — тот же игрок+клан+тир+исход повторно в пределах 3 с (двойная отправка);
      - is_not_member     — бой за клан, в котором игрок по событиям сейчас не состоит.
    has_tech_issue = любой из флагов. session_end_within_60s / pause_within_60s —
    контекст (приложение закрыли/свернули сразу после боя), в has_tech_issue не входит. -#}

with f as (
    select
        e.event_id as event_id, e.event_at as event_at, e.event_date as event_date, e.clan_id as clan_id,
        e.actor_id                                                    as player_id,
        e.boss_tier as boss_tier, e.is_win as is_win, e.hp_percent as hp_percent, e.rounds as rounds, e.app_version as app_version, e.country as country,
        p.stats_raw as stats_raw
    from {{ ref('int_clan_events') }} e
    left join (
        select id as event_id, properties.Stats::String as stats_raw
        from {{ source('petbuddy', 'events') }} final
        where event_date >= toDate('{{ var("clan_start_date", "2026-10-01") }}')
          and name = 'clan_raid_fight'
    ) p using (event_id)
    where e.event_name = 'clan_raid_fight'
),

flagged as (
    select
        *,
        JSONExtractFloat(stats_raw, 'CURRENT_HP')                     as stats_current_hp,
        JSONExtractFloat(stats_raw, 'MAX_HP')                         as stats_max_hp,
        dateDiff('millisecond',
                 lagInFrame(toNullable(event_at)) over (
                     partition by player_id, clan_id, boss_tier, is_win
                     order by event_at
                     rows between unbounded preceding and current row),
                 event_at)                                            as ms_since_same_fight
    from f
),

membership as (
    select f.event_id,
           countIf(f.event_at >= m.joined_at
                   and (m.left_at is null or f.event_at < m.left_at)) > 0 as is_member
    from f
    left join {{ ref('mart_clan_members') }} m
        on m.member_id = f.player_id and m.clan_id = f.clan_id
    group by f.event_id
),

ctx as (
    select f.event_id,
           countIf(c.name = 'session_end'
                   and c.event_at >= f.event_at
                   and c.event_at <= f.event_at + interval 60 second) > 0 as session_end_within_60s,
           countIf(c.name = 'pause'
                   and c.event_at >= f.event_at
                   and c.event_at <= f.event_at + interval 60 second) > 0 as pause_within_60s
    from f
    left join (
        select player_id, event_at, name
        from {{ source('petbuddy', 'events') }}
        where event_date >= toDate('{{ var("clan_start_date", "2026-10-01") }}')
          and name in ('session_end', 'pause')
          and player_id in (select player_id from f)
    ) c on c.player_id = f.player_id
    group by f.event_id
),

clan_names as (
    select clan_id, argMin(clan_name, event_at) as clan_name
    from {{ ref('int_clan_events') }}
    where event_name = 'clan_creation'
    group by clan_id
)

select
    x.event_id as event_id,
    x.event_at as event_at,
    x.event_date as event_date,
    x.clan_id as clan_id,
    cn.clan_name as clan_name,
    x.player_id as player_id,
    x.boss_tier as boss_tier,
    x.is_win as is_win,
    x.hp_percent as hp_percent,
    x.rounds as rounds,
    x.stats_current_hp as stats_current_hp,
    x.stats_max_hp as stats_max_hp,
    x.app_version as app_version,
    x.country as country,
    (x.is_win = 0 and x.hp_percent >= 100)                            as is_loss_full_hp,
    (x.stats_max_hp > 0 and x.hp_percent is not null
     and abs(x.stats_current_hp / x.stats_max_hp * 100 - x.hp_percent) > 1) as is_hp_mismatch,
    (x.boss_tier is null or x.hp_percent is null or x.is_win is null
     or x.stats_max_hp = 0)                                           as is_bad_payload,
    ifNull(x.ms_since_same_fight <= 3000, false)                      as is_duplicate,
    not ms.is_member                                                  as is_not_member,
    (is_loss_full_hp or is_hp_mismatch or is_bad_payload
     or is_duplicate or is_not_member)                                as has_tech_issue,
    multiIf(is_bad_payload, 'bad_payload',
            is_duplicate,   'duplicate',
            is_not_member,  'not_member',
            is_hp_mismatch, 'hp_mismatch',
            is_loss_full_hp,'loss_full_hp',
            null)                                                     as tech_issue_type,

    c.session_end_within_60s as session_end_within_60s,
    c.pause_within_60s as pause_within_60s,
    ifNull(dp.is_test_profile, false)                                 as is_test_profile
from flagged x
left join membership ms on ms.event_id = x.event_id
left join ctx c on c.event_id = x.event_id
left join clan_names cn on cn.clan_id = x.clan_id
left join {{ ref('dim_players') }} dp on dp.player_id = x.player_id
