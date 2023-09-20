{%- macro day_of_year(date) -%}
{{ adapter.dispatch('day_of_year', 'dbt_date') (date) }}
{%- endmacro %}

{%- macro default__day_of_year(date) -%}
    {{ dbt_date.date_part('dayofyear', date) }}
{%- endmacro %}

{%- macro postgres__day_of_year(date) -%}
    {{ dbt_date.date_part('doy', date) }}
{%- endmacro %}

{%- macro redshift__day_of_year(date) -%}
    cast({{ dbt_date.date_part('dayofyear', date) }} as {{ dbt.type_bigint() }})
{%- endmacro %}
