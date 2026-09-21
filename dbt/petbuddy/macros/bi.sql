{#- Хелперы для BI-витрин монетизации/экономики (дашборды Superset). -#}

{#- Бакет "возраста игрока" (Player Lifetime) по daysSinceRegistration.
    Совпадает с сеткой ТЗ: D0 / D1-3 / D4-7 / D8-14 / D15-30 / D31+. -#}
{% macro lifetime_bucket(days_col) -%}
  multiIf(
    {{ days_col }} is null, '(unknown)',
    {{ days_col }} = 0,  'D00',
    {{ days_col }} <= 3, 'D01-03',
    {{ days_col }} <= 7, 'D04-07',
    {{ days_col }} <= 14,'D08-14',
    {{ days_col }} <= 30,'D15-30',
    'D31+')
{%- endmacro %}

{#- Порядковый ключ бакета (для сортировки осей в Superset). -#}
{% macro lifetime_order(days_col) -%}
  multiIf(
    {{ days_col }} is null, 99,
    {{ days_col }} = 0,  0,
    {{ days_col }} <= 3, 1,
    {{ days_col }} <= 7, 2,
    {{ days_col }} <= 14,3,
    {{ days_col }} <= 30,4,
    5)
{%- endmacro %}

{#- Грубая группировка raw game-source (properties.source) в бизнес-категорию.
    ЭВРИСТИКА по наблюдаемым значениям — легко правится. Основное измерение на
    дашбордах всё равно raw source (реальное значение игры), category — опц. rollup. -#}
{% macro spend_category(src) -%}
  multiIf(
    {{ src }} in ('AdventurerSupplyBox','LegendarySupplyBox','Chests','ReleaseOfSpirits','ReleaseOfSpirits_RandomChest','RoulettePackShop','Roulette'), 'Gacha / Supply',
    {{ src }} in ('Revive'), 'Revive',
    {{ src }} in ('Pets','PetBuild'), 'Pet',
    {{ src }} in ('PurchaseEnergy','BuyResourcePanel','BuyResource'), 'Energy / Resources',
    {{ src }} in ('ShopPacks','ShopSoftPacks','ChapterPacks','BattlePass','7DaysMonetization'), 'Shop / Pass',
    {{ src }} in ('Skins_Unlock','TakeAllSkills'), 'Cosmetics',
    {{ src }} in ('ChapterRewards','Star','Bestiary','Spider Hollow','Pirate Hoard','AFKReward','Tasks','TalentFund','Roadmap'), 'Progression',
    {{ src }} in ('DailyBenefits','LoginReward'), 'Daily / Login',
    {{ src }} in ('DoubleReward','AdsDoubleReward','LuckyRefresh'), 'Rewards / Refresh',
    {{ src }} in ('Cheats'), 'Cheats / Test',
    'Other')
{%- endmacro %}
