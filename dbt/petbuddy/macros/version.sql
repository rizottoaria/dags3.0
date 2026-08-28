{#- Семантическое сравнение версий "MAJOR.MINOR.PATCH" (строковое неверно: '1.0.9' > '1.0.22').
    Пример: {{ version_gte('app_version', '1.0.22') }} -> app_version >= 1.0.22 по компонентам. -#}
{% macro version_gte(col, ver) -%}
  (
    toInt32OrZero(splitByChar('.', {{ col }})[1]),
    toInt32OrZero(splitByChar('.', {{ col }})[2]),
    toInt32OrZero(splitByChar('.', {{ col }})[3])
  ) >= ({{ ver.split('.')[0] }}, {{ ver.split('.')[1] }}, {{ ver.split('.')[2] }})
{%- endmacro %}
