{%- macro day_name(date, short=True) -%}
    {{ adapter.dispatch('day_name', 'dbt_date') (date, short) }}
{%- endmacro %}

{%- macro default__day_name(date, short) -%}
{%- set f = 'Dy' if short else 'Day' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro snowflake__day_name(date, short) -%}
    {%- if short -%}
    dayname({{ date }})
    {%- else -%}
    -- long version not implemented on Snowflake so we're doing it manually :/
    case dayname({{ date }})
        when 'Mon' then 'Monday'
        when 'Tue' then 'Tuesday'
        when 'Wed' then 'Wednesday'
        when 'Thu' then 'Thursday'
        when 'Fri' then 'Friday'
        when 'Sat' then 'Saturday'
        when 'Sun' then 'Sunday'
    end
    {%- endif -%}

{%- endmacro %}

{%- macro bigquery__day_name(date, short) -%}
{%- set f = '%a' if short else '%A' -%}
    format_date('{{ f }}', cast({{ date }} as date))
{%- endmacro %}

{%- macro postgres__day_name(date, short) -%}
{# FM = Fill mode, which suppresses padding blanks #}
{%- set f = 'FMDy' if short else 'FMDay' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro duckdb__day_name(date, short) -%}
    {%- if short -%}
    substr(dayname({{ date }}), 1, 3)
    {%- else -%}
    dayname({{ date }})
    {%- endif -%}
{%- endmacro %}
