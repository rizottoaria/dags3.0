-- Одна строка = одно платёжное событие (ad_reward или purchase).
select
    id                as event_id,
    player_id,
    session_id,
    event_at,
    event_date,
    country,
    app_version,
    ab_version,
    revenue_type,
    action_source     as revenue_source,
    revenue_amount
from {{ ref('stg_events') }}
where event_name = 'revenue'
  and revenue_amount is not null
