{{ config(materialized='view') }}
{#- Атрибуты игрока из аккаунта: рекламная кампания установки и install-профиль.
    Мост: events.player_id = PlayerProfile.id, PlayerProfile.userId = User.id.
    Одна строка на player_id. campaign_name = NULL, если аккаунт не найден
    (в ben только подмножество игроков) или кампания в User пустая — на витринах
    это сворачивается в '(unknown)'/'All Traffic'. -#}

with pp as (
    select
        JSONExtractString(data, 'id')     as player_id,
        JSONExtractString(data, 'userId') as user_id,
        JSONExtractBool(data, 'isTest')   as is_test_profile,
        nullIf(JSONExtractString(data, 'serverId'), '') as server_id
    from {{ source('raw', 'ben_player_profile') }}
    where JSONExtractString(data, 'id') != ''
),

usr as (
    select
        JSONExtractString(data, 'id')                        as user_id,
        nullIf(JSONExtractString(data, 'campaignName'), '')  as campaign_name,
        nullIf(JSONExtractString(data, 'externalId'), '')    as external_id,
        nullIf(JSONExtractString(data, 'appsFlyerId'), '')   as appsflyer_id,
        nullIf(JSONExtractString(data, 'country'), '')       as install_country,
        nullIf(JSONExtractString(data, 'platform'), '')      as platform,
        nullIf(JSONExtractString(data, 'firstVersion'), '')  as first_version,
        parseDateTimeBestEffortOrNull(
            JSONExtractString(data, 'createdAt'))            as user_created_at,
        -- deviceInfo = {"model": "samsung SM-S928B", "os": "Android OS 16 / API-36 (...)"};
        -- снимок последнего входа аккаунта (не на момент события), заполнен с ~августа 2026
        nullIf(JSONExtractString(JSONExtractString(data, 'deviceInfo'), 'model'), '') as device_model,
        nullIf(JSONExtractString(JSONExtractString(data, 'deviceInfo'), 'os'), '')    as device_os
    from {{ source('raw', 'ben_user') }}
)

select
    pp.player_id,
    pp.user_id,
    pp.is_test_profile,
    pp.server_id,
    u.campaign_name,
    u.external_id,
    u.appsflyer_id,
    u.install_country,
    u.platform,
    u.first_version,
    toDate(u.user_created_at) as install_date,
    u.device_model,
    nullIf(splitByChar(' ', ifNull(u.device_model, ''))[1], '')          as device_brand,
    u.device_os,
    nullIf(extract(ifNull(u.device_os, ''), '^([A-Za-z]+)'), '')          as os_name,
    nullIf(extract(ifNull(u.device_os, ''), '([0-9]+(?:[.][0-9]+)*)'), '') as os_version,
    toInt32OrNull(extract(ifNull(u.device_os, ''), 'API-([0-9]+)'))      as os_api_level
from pp
left join usr u using (user_id)
