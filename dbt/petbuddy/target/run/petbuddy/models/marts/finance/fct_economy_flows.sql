
        
  
    
    
    
        
         


        insert into `petbuddy_clean`.`fct_economy_flows__dbt_new_data_d01ec917_0ffb_4366_83fb_6668d06f2c84`
        ("event_id", "player_id", "session_id", "event_at", "event_date", "event_name", "country", "ab_version", "flow_source", "currency", "direction", "delta", "amount")

with __dbt__cte__int_events__economy as (
-- Разворачивает событие экономики в строки по каждой затронутой валюте.
-- Топ-уровневые ключи Currency_* / Chests_* в properties = дельта баланса
-- (balanceSnapshot лежит во вложенном объекте и сюда не попадает).
select
    id                                    as event_id,
    player_id,
    session_id,
    event_at,
    event_date,
    event_name,
    country,
    ab_version,
    action_source                         as flow_source,
    kv.1                                  as currency,
    kv.2                                  as delta,
    if(kv.2 >= 0, 'in', 'out')            as direction,
    abs(kv.2)                             as amount
from `petbuddy_clean`.`stg_events`
array join
    arrayFilter(
        x -> x.1 like 'Currency@_%' escape '@' or x.1 like 'Chests@_%' escape '@',
        JSONExtractKeysAndValues(toString(properties), 'Int64')
    ) as kv
where event_name in ('resource_top_up', 'resource_consume')
) -- Факт движения игровых валют: приход (source) и расход (sink) по каждой валюте.
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
from `__dbt__cte__int_events__economy`


where event_date >= (select max(event_date) from `petbuddy_clean`.`fct_economy_flows`) - toIntervalDay(2)

  
      