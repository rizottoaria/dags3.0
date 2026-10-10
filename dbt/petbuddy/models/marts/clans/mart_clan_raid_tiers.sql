{{ config(
    materialized='table',
    order_by='(clan_id)',
    tags=['marts','clans','bi']
) }}
{#- Прогресс клана по тирам рейдового босса: клан x bossTier. Сколько попыток ушло
    на тир, когда и за сколько часов он впервые пройден, винрейт, технические аномалии.
    Показывает «стену» сложности (тир, на котором клан застрял). -#}

select
    clan_id,
    any(clan_name)                                                    as clan_name,
    boss_tier,
    count()                                                           as attempts,
    countIf(is_win = 1)                                               as wins,
    countIf(is_win = 0)                                               as losses,
    round(wins / attempts, 4)                                         as win_rate,
    uniqExact(player_id)                                              as fighters,
    uniqExactIf(player_id, is_win = 1)                                as winners,
    min(event_at)                                                     as first_attempt_at,
    minIf(toNullable(event_at), is_win = 1)                           as first_win_at,
    first_win_at is not null                                          as is_cleared,
    round(dateDiff('second', first_attempt_at, first_win_at) / 3600, 2) as hours_to_first_win,
    countIf(tier_first_win_at is null or event_at < tier_first_win_at) as attempts_before_first_win,
    round(avg(rounds), 2)                                             as avg_rounds,
    round(avgIf(hp_percent, is_win = 1), 2)                           as avg_hp_percent_on_win,
    countIf(has_tech_issue)                                           as tech_issue_fights,
    countIf(is_loss_full_hp)                                          as loss_full_hp_fights
from (
    select
        *,
        min(if(is_win = 1, toNullable(event_at), null))
            over (partition by clan_id, boss_tier)                    as tier_first_win_at
    from {{ ref('mart_clan_raid_fights') }}
)
group by clan_id, boss_tier
