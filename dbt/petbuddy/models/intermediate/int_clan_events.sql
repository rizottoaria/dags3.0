{#- Клановые события в плоском виде (фича кланов, первые события 02.10.2026).
    Читаем НАПРЯМУЮ из источника (FINAL) только нужные суб-колонки, с отсечкой по
    event_date/name (они в ORDER BY events — чтение дешёвое). Тестовые события
    (properties.isTest = true) отбрасываем.

    member_id — над кем действие: joinUserId / kickUserId / changeUserId, иначе сам
    актор (создатель клана, участник рейда, апнувший уровень).
    Явного события «выход из клана» нет: уход = clan_kick или вступление/создание
    другого клана (см. mart_clan_members). -#}

select
    id                                                         as event_id,
    event_at,
    toDate(event_at)                                           as event_date,
    name                                                       as event_name,
    player_id                                                  as actor_id,
    properties.clanId::String                                  as clan_id,
    coalesce(
        nullIf(properties.joinUserId::String, ''),
        nullIf(properties.kickUserId::String, ''),
        nullIf(properties.changeUserId::String, ''),
        player_id)                                             as member_id,
    nullIf(properties.clanName::String, '')                    as clan_name,
    nullIf(properties.joinMethod::String, '')                  as join_method,
    toInt32OrNull(properties.memberCountAfterJoin::String)     as member_count_after,
    toInt32OrNull(properties.newLevel::String)                 as new_level,
    nullIf(properties.oldRole::String, '')                     as old_role,
    nullIf(properties.newRole::String, '')                     as new_role,
    toInt32OrNull(properties.bossTier::String)                 as boss_tier,
    multiIf(lower(properties.IsWin::String) = 'true', 1,
            lower(properties.IsWin::String) = 'false', 0, null) as is_win,
    toFloat64OrNull(properties.HpPercent::String)              as hp_percent,
    toInt32OrNull(properties.Rounds::String)                   as rounds,
    nullIf(properties.version::String, '')                     as app_version,
    nullIf(properties.country::String, '')                     as country
from {{ source('petbuddy', 'events') }} final
where event_date >= toDate('{{ var("clan_start_date", "2026-10-01") }}')
  and name in ('clan_creation', 'clan_join', 'clan_kick', 'clan_role_change',
               'clan_lvl_up', 'clan_raid_fight')
  and properties.isTest::String != 'true'
  and properties.clanId::String != ''
