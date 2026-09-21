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
            JSONExtractString(data, 'createdAt'))            as user_created_at
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
    toDate(u.user_created_at) as install_date
from pp
left join usr u using (user_id)
