{{ config(materialized='table', order_by='(event_date)') }}

-- Ежедневная активность: DAU, новые/вернувшиеся, сессии и выручка на день.
with daily as (
    select
        event_date,
        player_id,
        session_id,
        revenue_amount,
        event_name
    from {{ ref('stg_events') }}
),

firsts as (
    select player_id, first_seen_date
    from {{ ref('dim_players') }}
)

select
    d.event_date                                                       as event_date,
    uniqExact(d.player_id)                                             as dau,
    uniqExactIf(d.player_id, d.event_date = f.first_seen_date)         as new_users,
    uniqExactIf(d.player_id, d.event_date > f.first_seen_date)         as returning_users,
    uniqExact(d.session_id)                                            as sessions,
    round(sumIf(d.revenue_amount, d.event_name = 'revenue'), 4)        as revenue,
    round(
        sumIf(d.revenue_amount, d.event_name = 'revenue')
            / nullIf(uniqExact(d.player_id), 0),
        4)                                                             as arpdau
from daily d
inner join firsts f using (player_id)
group by d.event_date
order by d.event_date
