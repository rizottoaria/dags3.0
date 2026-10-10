{{ config(
    materialized='table',
    order_by='(clan_id, event_date)',
    tags=['marts','clans','bi']
) }}
{#- Дневной прогресс клана: клан x день, строки уплотнены от первого события клана
    до сегодня (дни без событий тоже есть — численность/уровень/тир тянутся вперёд).
    Численность на конец дня — по периодам членства (mart_clan_members), активные
    участники — состоявшие в клане игроки с любой активностью в этот день (mart_bi_activity). -#}

with ev as (
    select * from {{ ref('int_clan_events') }}
),

calendar as (
    select
        clan_id,
        addDays(first_date, arrayJoin(range(toUInt32(dateDiff('day', first_date, today()) + 1)))) as event_date
    from (select clan_id, min(event_date) as first_date from ev group by clan_id)
),

day_events as (
    select
        clan_id, event_date,
        countIf(event_name = 'clan_join')                             as joins,
        countIf(event_name = 'clan_kick')                             as kicks,
        countIf(event_name = 'clan_role_change')                      as role_changes,
        maxIf(new_level, event_name = 'clan_lvl_up')                  as level_up_to
    from ev
    group by clan_id, event_date
),

day_raids as (
    select
        clan_id, event_date,
        count()                                                       as raid_fights,
        countIf(is_win = 1)                                           as raid_wins,
        uniqExact(player_id)                                          as raid_fighters,
        maxIf(boss_tier, is_win = 1)                                  as max_tier_won_day,
        countIf(has_tech_issue)                                       as tech_issue_fights,
        countIf(is_loss_full_hp)                                      as loss_full_hp_fights
    from {{ ref('mart_clan_raid_fights') }}
    group by clan_id, event_date
),

first_clears as (
    select clan_id, toDate(first_win_at) as event_date, count() as tiers_first_cleared
    from {{ ref('mart_clan_raid_tiers') }}
    where is_cleared
    group by clan_id, event_date
),

members_eod as (
    select
        c.clan_id as clan_id, c.event_date as event_date,
        uniqExactIf(m.member_id, m.joined_date <= c.event_date
                    and (m.left_date is null or m.left_date > c.event_date)) as members_eod,
        countIf(m.left_date = c.event_date and m.leave_reason = 'switched_clan') as left_switched
    from calendar c
    left join {{ ref('mart_clan_members') }} m on m.clan_id = c.clan_id
    group by c.clan_id, c.event_date
),

active as (
    select m.clan_id, a.event_date, uniqExact(a.player_id) as active_members
    from {{ ref('mart_bi_activity') }} a
    inner join {{ ref('mart_clan_members') }} m on m.member_id = a.player_id
    where a.event_date >= toDate('{{ var("clan_start_date", "2026-10-01") }}')
      and a.event_date >= m.joined_date
      and (m.left_date is null or a.event_date <= m.left_date)
    group by m.clan_id, a.event_date
),

clan_names as (
    select clan_id, argMin(clan_name, event_at) as clan_name
    from ev where event_name = 'clan_creation'
    group by clan_id
),

joined as (
    select
        c.clan_id as clan_id,
        cn.clan_name as clan_name,
        c.event_date as event_date,
        ifNull(de.joins, 0)                                           as joins,
        ifNull(de.kicks, 0)                                           as kicks,
        me.left_switched as left_switched,
        ifNull(de.role_changes, 0)                                    as role_changes,
        me.members_eod as members_eod,
        ifNull(ac.active_members, 0)                                  as active_members,
        greatest(1, max(ifNull(de.level_up_to, 0)) over w)            as clan_level_eod,
        ifNull(dr.raid_fights, 0)                                     as raid_fights,
        ifNull(dr.raid_wins, 0)                                       as raid_wins,
        ifNull(dr.raid_fighters, 0)                                   as raid_fighters,
        ifNull(fc.tiers_first_cleared, 0)                             as tiers_first_cleared,
        max(ifNull(dr.max_tier_won_day, 0)) over w                    as max_tier_cleared_cum,
        ifNull(dr.tech_issue_fights, 0)                               as tech_issue_fights,
        ifNull(dr.loss_full_hp_fights, 0)                             as loss_full_hp_fights
    from calendar c
    left join day_events de  on de.clan_id = c.clan_id and de.event_date = c.event_date
    left join day_raids dr   on dr.clan_id = c.clan_id and dr.event_date = c.event_date
    left join first_clears fc on fc.clan_id = c.clan_id and fc.event_date = c.event_date
    left join members_eod me on me.clan_id = c.clan_id and me.event_date = c.event_date
    left join active ac      on ac.clan_id = c.clan_id and ac.event_date = c.event_date
    left join clan_names cn  on cn.clan_id = c.clan_id
    window w as (partition by c.clan_id order by c.event_date
                 rows between unbounded preceding and current row)
)

select
    *,
    if(members_eod > 0, round(active_members / members_eod, 4), null) as active_share,
    if(members_eod > 0, round(raid_fighters / members_eod, 4), null)  as raid_participation,
    if(raid_fights > 0, round(tech_issue_fights / raid_fights, 4), null) as tech_issue_rate
from joined
