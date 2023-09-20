{% macro rand() -%}
    {{ adapter.dispatch('rand', 'dbt_expectations') () }}
{%- endmacro %}

{% macro default__rand() -%}

    rand()

{%- endmacro -%}

{% macro bigquery__rand() -%}

    rand()

{%- endmacro -%}

{% macro snowflake__rand(seed) -%}

    uniform(0::float, 1::float, random())

{%- endmacro -%}

{% macro postgres__rand() -%}

    random()

{%- endmacro -%}

{% macro redshift__rand() -%}

    random()

{%- endmacro -%}

{% macro duckdb__rand() -%}

    random()

{%- endmacro -%}
