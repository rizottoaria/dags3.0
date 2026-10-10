{{ config(
    materialized='table',
    order_by='(clan_id)',
    tags=['marts','clans','bi']
) }}
{#- Текущее состояние клана: одна строка на клан. Возраст, уровень, численность
    (текущая/пиковая/за всё время), отток, рейдовый прогресс, активность за 7 дней,
    технические аномалии рейдов. -#}

with ev as (
    select * from {{ ref('int_clan_events') }}
),

base as (
    select
        clan_id,
        argMinIf(clan_name, event_at, event_name = 'clan_creation')   as clan_name,
        argMinIf(actor_id, event_at, event_name = 'clan_creation')    as creator_id,
        minIf(toNullable(event_at), event_name = 'clan_creation')     as created_at,
        min(event_at)                                                 as first_event_at,
        max(event_at)                                                 as last_event_at,
        greatest(1, maxIf(new_level, event_name = 'clan_lvl_up'))     as clan_level,
        maxIf(toNullable(event_at), event_name = 'clan_lvl_up')       as last_level_up_at,
        countIf(event_name = 'clan_join')                             as joins_total,
        countIf(event_name = 'clan_kick')                             as kicks_total
    from ev
    group by clan_id
),

members as (
    select
        clan_id,
        uniqExactIf(member_id, is_current)                            as members_current,
        uniqExact(member_id)                                          as members_ever,
        countIf(leave_reason = 'switched_clan')                       as switched_out_total,
        argMaxIf(member_id, joined_at, is_current and role = 'LEADER') as leader_id
    from {{ ref('mart_clan_members') }}
    group by clan_id
),

daily as (
    select
        clan_id,
        max(members_eod)                                              as members_peak,
        sumIf(raid_fights, event_date > today() - 7)                  as raid_fights_7d
    from {{ ref('mart_clan_daily') }}
    group by clan_id
),

active7 as (
    select m.clan_id, uniqExact(a.player_id) as active_members_7d
    from {{ ref('mart_bi_activity') }} a
    inner join {{ ref('mart_clan_members') }} m on m.member_id = a.player_id
    where m.is_current and a.event_date > today() - 7
    group by m.clan_id
),

raids as (
    select
        clan_id,
        count()                                                       as raid_fights_total,
        countIf(is_win = 1)                                           as raid_wins_total,
        uniqExact(player_id)                                          as raid_fighters_total,
        nullIf(maxIf(boss_tier, is_win = 1), 0)                       as max_tier_cleared,
        max(event_at)                                                 as last_raid_at,
        countIf(has_tech_issue)                                       as tech_issue_fights_total,
        countIf(is_loss_full_hp)                                      as loss_full_hp_fights_total
    from {{ ref('mart_clan_raid_fights') }}
    group by clan_id
),

last_clear as (
    select clan_id, argMax(first_win_at, boss_tier) as max_tier_cleared_at
    from {{ ref('mart_clan_raid_tiers') }}
    where is_cleared
    group by clan_id
)

select
    b.clan_id as clan_id,
    b.clan_name as clan_name,
    b.creator_id as creator_id,
    b.created_at as created_at,
    b.first_event_at as first_event_at,
    b.last_event_at as last_event_at,
    dateDiff('day', toDate(coalesce(b.created_at, b.first_event_at)), today()) as clan_age_days,
    b.clan_level as clan_level,
    b.last_level_up_at as last_level_up_at,
    m.leader_id as leader_id,
    m.members_current as members_current,
    d.members_peak as members_peak,
    m.members_ever as members_ever,
    b.joins_total as joins_total,
    b.kicks_total as kicks_total,
    m.switched_out_total as switched_out_total,
    ifNull(a7.active_members_7d, 0)                                   as active_members_7d,
    if(m.members_current > 0,
       round(ifNull(a7.active_members_7d, 0) / m.members_current, 4), null) as active_share_7d,
    ifNull(r.raid_fights_total, 0)                                    as raid_fights_total,
    ifNull(r.raid_wins_total, 0)                                      as raid_wins_total,
    if(r.raid_fights_total > 0, round(r.raid_wins_total / r.raid_fights_total, 4), null) as raid_win_rate,
    ifNull(r.raid_fighters_total, 0)                                  as raid_fighters_total,
    d.raid_fights_7d as raid_fights_7d,
    r.max_tier_cleared as max_tier_cleared,
    lc.max_tier_cleared_at as max_tier_cleared_at,
    r.last_raid_at as last_raid_at,
    ifNull(r.tech_issue_fights_total, 0)                              as tech_issue_fights_total,
    ifNull(r.loss_full_hp_fights_total, 0)                            as loss_full_hp_fights_total,
    if(r.raid_fights_total > 0,
       round(r.tech_issue_fights_total / r.raid_fights_total, 4), null) as tech_issue_rate,
    ifNull(dp.is_test_profile, false)                                 as is_test_creator
from base b
left join members m   on m.clan_id = b.clan_id
left join daily d     on d.clan_id = b.clan_id
left join active7 a7  on a7.clan_id = b.clan_id
left join raids r     on r.clan_id = b.clan_id
left join last_clear lc on lc.clan_id = b.clan_id
left join {{ ref('dim_players') }} dp on dp.player_id = b.creator_id
