{% macro date_part(datepart, date) -%}
    {{ adapter.dispatch('date_part', 'dbt_date') (datepart, date) }}
{%- endmacro %}

{% macro default__date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{  date }})
{%- endmacro %}

{% macro bigquery__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}
