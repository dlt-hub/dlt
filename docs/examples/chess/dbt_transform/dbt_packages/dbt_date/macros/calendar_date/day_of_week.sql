{%- macro day_of_week(date, isoweek=true) -%}
{{ adapter.dispatch('day_of_week', 'dbt_date') (date, isoweek) }}
{%- endmacro %}

{%- macro default__day_of_week(date, isoweek) -%}

    {%- set dow = dbt_date.date_part('dayofweek', date) -%}

    {%- if isoweek -%}
    case
        -- Shift start of week from Sunday (0) to Monday (1)
        when {{ dow }} = 0 then 7
        else {{ dow }}
    end
    {%- else -%}
    {{ dow }} + 1
    {%- endif -%}

{%- endmacro %}

{%- macro snowflake__day_of_week(date, isoweek) -%}

    {%- if isoweek -%}
        {%- set dow_part = 'dayofweekiso' -%}
        {{ dbt_date.date_part(dow_part, date) }}
    {%- else -%}
        {%- set dow_part = 'dayofweek' -%}
        case
            when {{ dbt_date.date_part(dow_part, date) }} = 7 then 1
            else {{ dbt_date.date_part(dow_part, date) }} + 1
        end
    {%- endif -%}



{%- endmacro %}

{%- macro bigquery__day_of_week(date, isoweek) -%}

    {%- set dow = dbt_date.date_part('dayofweek', date) -%}

    {%- if isoweek -%}
    case
        -- Shift start of week from Sunday (1) to Monday (2)
        when {{ dow }} = 1 then 7
        else {{ dow }} - 1
    end
    {%- else -%}
    {{ dow }}
    {%- endif -%}

{%- endmacro %}


{%- macro postgres__day_of_week(date, isoweek) -%}

    {%- if isoweek -%}
        {%- set dow_part = 'isodow' -%}
        -- Monday(1) to Sunday (7)
        cast({{ dbt_date.date_part(dow_part, date) }} as {{ dbt.type_int() }})
    {%- else -%}
        {%- set dow_part = 'dow' -%}
        -- Sunday(1) to Saturday (7)
        cast({{ dbt_date.date_part(dow_part, date) }} + 1 as {{ dbt.type_int() }})
    {%- endif -%}

{%- endmacro %}


{%- macro redshift__day_of_week(date, isoweek) -%}

    {%- set dow = dbt_date.date_part('dayofweek', date) -%}

    {%- if isoweek -%}
    case
        -- Shift start of week from Sunday (0) to Monday (1)
        when {{ dow }} = 0 then 7
        else cast({{ dow }} as {{ dbt.type_bigint() }})
    end
    {%- else -%}
    cast({{ dow }} + 1 as {{ dbt.type_bigint() }})
    {%- endif -%}

{%- endmacro %}

{%- macro duckdb__day_of_week(date, isoweek) -%}
{{ return(dbt_date.postgres__day_of_week(date, isoweek)) }}
{%- endmacro %}
