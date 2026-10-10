{{ config(
    materialized='table',
    order_by='(clan_id, member_id, joined_at)',
    tags=['marts','clans','bi']
) }}
{#- Членство игрока в клане: одна строка = один период пребывания (игрок может
    вернуться в тот же клан — тогда строк несколько, ключ member_id+clan_id+joined_at).

    Начало: clan_creation (создатель) или clan_join.
    Конец: clan_kick этого игрока из этого клана, иначе следующее вступление/создание
    этим игроком (= перешёл в другой клан). Явного «вышел сам» в событиях нет, поэтому
    добровольный выход без перехода не виден — игрок числится в клане.
    Роль: последняя clan_role_change за период, по умолчанию LEADER у создателя и
    MEMBER у остальных. Рейдовая статистика — бои игрока за этот клан внутри периода. -#}

with ev as (
    select * from {{ ref('int_clan_events') }}
),

starts as (
    select
        member_id,
        clan_id,
        event_at                                                      as joined_at,
        if(event_name = 'clan_creation', 'creator',
           ifNull(join_method, '(unknown)'))                          as join_method,
        app_version                                                   as join_app_version,
        country                                                       as join_country
    from ev
    where event_name in ('clan_creation', 'clan_join')
),

with_next as (
    select
        *,
        leadInFrame(toNullable(joined_at)) over (
            partition by member_id order by joined_at
            rows between unbounded preceding and unbounded following) as next_start_at
    from starts
),

kicks as (
    select member_id, clan_id, event_at as kick_at, actor_id as kicked_by
    from ev where event_name = 'clan_kick'
),

memberships as (
    select
        w.member_id as member_id, w.clan_id as clan_id, w.joined_at as joined_at, w.join_method as join_method, w.join_app_version as join_app_version, w.join_country as join_country, w.next_start_at as next_start_at,
        minIf(toNullable(k.kick_at),
              k.kick_at > w.joined_at
              and (w.next_start_at is null or k.kick_at < w.next_start_at)) as kick_at,
        argMinIf(toNullable(k.kicked_by), k.kick_at,
              k.kick_at > w.joined_at
              and (w.next_start_at is null or k.kick_at < w.next_start_at)) as kicked_by
    from with_next w
    left join kicks k on k.member_id = w.member_id and k.clan_id = w.clan_id
    group by w.member_id, w.clan_id, w.joined_at, w.join_method,
             w.join_app_version, w.join_country, w.next_start_at
),

m as (
    select
        *,
        coalesce(kick_at, next_start_at)                              as left_at,
        multiIf(kick_at is not null, 'kicked',
                next_start_at is not null, 'switched_clan', null)    as leave_reason
    from memberships
),

roles as (
    select
        m.member_id as member_id, m.clan_id as clan_id, m.joined_at as joined_at,
        argMaxIf(toNullable(r.new_role), r.event_at,
                 r.event_at >= m.joined_at
                 and (m.left_at is null or r.event_at < m.left_at))   as last_role,
        countIf(r.event_at >= m.joined_at
                and (m.left_at is null or r.event_at < m.left_at))    as role_changes
    from m
    left join (select member_id, clan_id, event_at, new_role
               from ev where event_name = 'clan_role_change') r
        on r.member_id = m.member_id and r.clan_id = m.clan_id
    group by m.member_id, m.clan_id, m.joined_at
),

raids as (
    select
        member_id, clan_id, joined_at,
        countIf(in_period)                                            as raid_fights,
        countIf(in_period and is_win = 1)                             as raid_wins,
        nullIf(maxIf(boss_tier, in_period and is_win = 1), 0)         as max_boss_tier_won,
        maxIf(toNullable(fight_at), in_period)                        as last_raid_at,
        uniqExactIf(fight_date, in_period)                            as raid_days
    from (
        select
            m.member_id as member_id, m.clan_id as clan_id, m.joined_at as joined_at,
            f.event_at as fight_at, f.event_date as fight_date, f.boss_tier, f.is_win,
            f.event_at >= m.joined_at
                and (m.left_at is null or f.event_at < m.left_at)     as in_period
        from m
        left join (select actor_id, clan_id, event_at, event_date, boss_tier, is_win
                   from ev where event_name = 'clan_raid_fight') f
            on f.actor_id = m.member_id and f.clan_id = m.clan_id
    )
    group by member_id, clan_id, joined_at
),

clan_names as (
    select clan_id, argMin(clan_name, event_at) as clan_name
    from ev where event_name = 'clan_creation'
    group by clan_id
)

select
    m.clan_id as clan_id,
    cn.clan_name as clan_name,
    m.member_id as member_id,
    m.joined_at as joined_at,
    toDate(m.joined_at)                                               as joined_date,
    m.join_method as join_method,
    m.join_method = 'creator'                                         as is_creator,
    m.left_at as left_at,
    toDate(m.left_at)                                                 as left_date,
    m.leave_reason as leave_reason,
    m.kicked_by as kicked_by,
    m.left_at is null                                                 as is_current,
    round(dateDiff('second', m.joined_at,
                   coalesce(m.left_at, now64(3))) / 86400, 2)         as days_in_clan,
    coalesce(r.last_role,
             if(m.join_method = 'creator', 'LEADER', 'MEMBER'))       as role,
    r.role_changes as role_changes,
    rd.raid_fights as raid_fights,
    rd.raid_wins as raid_wins,
    if(rd.raid_fights > 0, round(rd.raid_wins / rd.raid_fights, 4), null) as raid_win_rate,
    rd.max_boss_tier_won as max_boss_tier_won,
    rd.raid_days as raid_days,
    rd.last_raid_at as last_raid_at,
    m.join_app_version as join_app_version,
    m.join_country as join_country,
    ifNull(dp.is_test_profile, false)                                 as is_test_profile
from m
left join roles r
    on r.member_id = m.member_id and r.clan_id = m.clan_id and r.joined_at = m.joined_at
left join raids rd
    on rd.member_id = m.member_id and rd.clan_id = m.clan_id and rd.joined_at = m.joined_at
left join clan_names cn on cn.clan_id = m.clan_id
left join {{ ref('dim_players') }} dp on dp.player_id = m.member_id
