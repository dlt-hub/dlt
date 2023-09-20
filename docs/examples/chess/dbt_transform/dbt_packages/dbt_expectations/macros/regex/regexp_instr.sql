{% macro regexp_instr(source_value, regexp, position=1, occurrence=1, is_raw=False, flags="") %}

    {{ adapter.dispatch('regexp_instr', 'dbt_expectations')(
        source_value, regexp, position, occurrence, is_raw, flags
    ) }}

{% endmacro %}

{% macro default__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{# unclear if other databases support raw strings or flags #}
{% if is_raw or flags %}
    {{ exceptions.warn(
            "is_raw and flags options are not supported for this adapter "
            ~ "and are being ignored."
    ) }}
{% endif %}
regexp_instr({{ source_value }}, '{{ regexp }}', {{ position }}, {{ occurrence }})
{% endmacro %}

{# Snowflake uses $$...$$ to escape raw strings #}
{% macro snowflake__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{%- set regexp = "$$" ~ regexp ~ "$$" if is_raw else "'" ~ regexp ~ "'" -%}
{% if flags %}{{ dbt_expectations._validate_flags(flags, 'cimes') }}{% endif %}
regexp_instr({{ source_value }}, {{ regexp }}, {{ position }}, {{ occurrence }}, 0, '{{ flags }}')
{% endmacro %}

{# BigQuery uses "r" to escape raw strings #}
{% macro bigquery__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{% if flags %}
    {{ dbt_expectations._validate_re2_flags(flags) }}
    {# BigQuery prepends "(?flags)" to set flags for current group #}
    {%- set regexp = "(?" ~ flags ~ ")" ~ regexp -%}
{% endif %}
{%- set regexp = "r'" ~ regexp ~ "'" if is_raw else "'" ~ regexp ~ "'" -%}
regexp_instr({{ source_value }}, {{ regexp }}, {{ position }}, {{ occurrence }})
{% endmacro %}

{# Postgres does not need to escape raw strings #}
{% macro postgres__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{% if flags %}{{ dbt_expectations._validate_flags(flags, 'bcegimnpqstwx') }}{% endif %}
coalesce(array_length((select regexp_matches({{ source_value }}, '{{ regexp }}', '{{ flags }}')), 1), 0)
{% endmacro %}

{# Unclear what Redshift does to escape raw strings #}
{% macro redshift__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{% if flags %}{{ dbt_expectations._validate_flags(flags, 'ciep') }}{% endif %}
regexp_instr({{ source_value }}, '{{ regexp }}', {{ position }}, {{ occurrence }}, 0, '{{ flags }}')
{% endmacro %}

{% macro duckdb__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{% if flags %}{{ dbt_expectations._validate_flags(flags, 'ciep') }}{% endif %}
regexp_matches({{ source_value }}, '{{ regexp }}', '{{ flags }}')
{% endmacro %}


{% macro _validate_flags(flags, alphabet) %}
{% for flag in flags %}
    {% if flag not in alphabet %}
    {# Using raise_compiler_error causes disabled tests with invalid flags to fail compilation #}
    {{ exceptions.warn(
        "flag " ~ flag ~ " not in list of allowed flags for this adapter: " ~ alphabet | join(", ")
    ) }}
    {% endif %}
{% endfor %}
{% endmacro %}

{# Re2 requires specific flag validation because of its clear flag operator #}
{% macro _validate_re2_flags(flags) %}
{# Re2 supports following flags: #}
{# i  :  case-insensitive (default fault) #}
{# m  :  multi-line mode: ^ and $ match begin/end line in addition to begin/end text (default false) #}
{# s  :  let . match \n (default false) #}
{# U  :  ungreedy: swap meaning of x* and x*?, x+ and x+?, etc (default false) #}
{# Flag syntax is xyz (set) or -xyz (clear) or xy-z (set xy, clear z).  #}

{# Regex explanation: do not allow consecutive dashes, accept all re2 flags and clear operator, do not end with a dash  #}
{% set re2_flags_pattern = '^(?!.*--)[-imsU]*(?<!-)$' %}
{% set re = modules.re %}
{% set is_match = re.match(re2_flags_pattern, flags) %}
{% if not is_match %}
    {# Using raise_compiler_error causes disabled tests with invalid flags to fail compilation #}
    {{ exceptions.warn(
        "flags " ~ flags ~ " isn't a valid re2 flag pattern"
    ) }}
{% endif %}
{% endmacro %}
