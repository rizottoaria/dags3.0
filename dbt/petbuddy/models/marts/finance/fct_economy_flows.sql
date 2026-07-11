{{ config(materialized='table', order_by='(event_date, currency)') }}

-- Факт движения игровых валют: приход (source) и расход (sink) по каждой валюте.
select
    event_id,
    player_id,
    session_id,
    event_at,
    event_date,
    event_name,
    country,
    ab_version,
    flow_source,
    currency,
    direction,
    delta,
    amount
from {{ ref('int_events__economy') }}
