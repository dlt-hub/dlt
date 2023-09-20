{%- macro from_unixtimestamp(epochs, format="seconds") -%}
    {{ adapter.dispatch('from_unixtimestamp', 'dbt_date') (epochs, format) }}
{%- endmacro %}

{%- macro default__from_unixtimestamp(epochs, format="seconds") -%}
    {%- if format != "seconds" -%}
    {{ exceptions.raise_compiler_error(
        "value " ~ format ~ " for `format` for from_unixtimestamp is not supported."
        )
    }}
    {% endif -%}
    to_timestamp({{ epochs }})
{%- endmacro %}

{%- macro postgres__from_unixtimestamp(epochs, format="seconds") -%}
    {%- if format != "seconds" -%}
    {{ exceptions.raise_compiler_error(
        "value " ~ format ~ " for `format` for from_unixtimestamp is not supported."
        )
    }}
    {% endif -%}
    cast(to_timestamp({{ epochs }}) at time zone 'UTC' as timestamp)
{%- endmacro %}

{%- macro snowflake__from_unixtimestamp(epochs, format) -%}
    {%- if format == "seconds" -%}
    {%- set scale = 0 -%}
    {%- elif format == "milliseconds" -%}
    {%- set scale = 3 -%}
    {%- elif format == "microseconds" -%}
    {%- set scale = 6 -%}
    {%- else -%}
    {{ exceptions.raise_compiler_error(
        "value " ~ format ~ " for `format` for from_unixtimestamp is not supported."
        )
    }}
    {% endif -%}
    to_timestamp_ntz({{ epochs }}, {{ scale }})

{%- endmacro %}

{%- macro bigquery__from_unixtimestamp(epochs, format) -%}
    {%- if format == "seconds" -%}
        timestamp_seconds({{ epochs }})
    {%- elif format == "milliseconds" -%}
        timestamp_millis({{ epochs }})
    {%- elif format == "microseconds" -%}
        timestamp_micros({{ epochs }})
    {%- else -%}
    {{ exceptions.raise_compiler_error(
        "value " ~ format ~ " for `format` for from_unixtimestamp is not supported."
        )
    }}
    {% endif -%}
{%- endmacro %}
