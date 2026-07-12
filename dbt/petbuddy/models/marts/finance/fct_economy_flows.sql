{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['event_id', 'currency'],
    order_by='(event_date, currency)',
    query_settings={'max_threads': 2}
  )
}}

-- Факт движения игровых валют: приход (source) и расход (sink) по каждой валюте.
-- Инкрементально: при обычном run обрабатываем только последние event_date
-- (окно 2 дня — на случай долетающих/переигранных событий), а delete+insert
-- по (event_id, currency) заменяет строки этих дней. Полный пересбор: --full-refresh.
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

{% if is_incremental() %}
where event_date >= (select max(event_date) from {{ this }}) - toIntervalDay(2)
{% endif %}
