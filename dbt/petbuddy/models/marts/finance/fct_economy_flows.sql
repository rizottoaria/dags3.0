{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['event_id', 'currency'],
    order_by='(event_date, currency)',
    query_settings={'max_threads': 2, 'do_not_merge_across_partitions_select_final': 1}
  )
}}

-- Факт движения игровых валют: приход (source) и расход (sink) по каждой валюте.
-- Инкрементально: при обычном run обрабатываем только последние event_date
-- (окно 2 дня — на случай долетающих/переигранных событий), а delete+insert
-- по (event_id, currency) заменяет строки этих дней. Полный пересбор: --full-refresh.
select
    e.event_id,
    e.player_id,
    e.session_id,
    e.event_at,
    e.event_date,
    e.event_name,
    e.country,
    e.ab_version,
    e.flow_source,
    e.currency,
    e.direction,
    e.delta,
    e.amount,
    ifNull(pc.campaign_name, '(unknown)') as marketing_campaign
from {{ ref('int_events__economy') }} e
left join {{ ref('int_player_campaign') }} pc using (player_id)

{% if is_incremental() %}
where e.event_date >= (select max(event_date) from {{ this }}) - toIntervalDay(2)
{% endif %}
