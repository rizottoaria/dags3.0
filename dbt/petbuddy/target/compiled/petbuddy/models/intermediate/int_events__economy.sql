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