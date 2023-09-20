{%- macro month_name(date, short=True) -%}
    {{ adapter.dispatch('month_name', 'dbt_date') (date, short) }}
{%- endmacro %}

{%- macro default__month_name(date, short) -%}
{%- set f = 'MON' if short else 'MONTH' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro bigquery__month_name(date, short) -%}
{%- set f = '%b' if short else '%B' -%}
    format_date('{{ f }}', cast({{ date }} as date))
{%- endmacro %}

{%- macro snowflake__month_name(date, short) -%}
{%- set f = 'MON' if short else 'MMMM' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro postgres__month_name(date, short) -%}
{# FM = Fill mode, which suppresses padding blanks #}
{%- set f = 'FMMon' if short else 'FMMonth' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}


{%- macro duckdb__month_name(date, short) -%}
    {%- if short -%}
    substr(monthname({{ date }}), 1, 3)
    {%- else -%}
    monthname({{ date }})
    {%- endif -%}
{%- endmacro %}
