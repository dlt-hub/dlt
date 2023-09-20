{%- test expect_table_column_count_to_be_between(model,
                                                    min_value=None,
                                                    max_value=None
                                                    ) -%}
{%- if min_value is none and max_value is none -%}
{{ exceptions.raise_compiler_error(
    "You have to provide either a min_value, max_value or both."
) }}
{%- endif -%}
{%- if execute -%}
{%- set number_actual_columns = (adapter.get_columns_in_relation(model) | length) -%}

{%- set expression %}
( 1=1
{%- if min_value %} and number_actual_columns >= min_value{% endif %}
{%- if max_value %} and number_actual_columns <= max_value{% endif %}
)
{% endset -%}

with test_data as (

    select
        {{ number_actual_columns }} as number_actual_columns,
        {{ min_value if min_value else 0 }} as min_value,
        {{ max_value if max_value else 0 }} as max_value

)
select *
from test_data
where
    not {{ expression }}
{%- endif -%}
{%- endtest -%}
