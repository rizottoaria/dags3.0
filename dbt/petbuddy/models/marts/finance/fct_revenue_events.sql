{{ config(materialized='table', order_by='(event_date, player_id)') }}

-- Факт-таблица платёжных событий (одна строка = один платёж).
select
    event_id,
    player_id,
    session_id,
    event_at,
    event_date,
    country,
    app_version,
    ab_version,
    revenue_type,
    revenue_source,
    round(revenue_amount, 6) as revenue_amount
from {{ ref('int_events__revenue') }}
