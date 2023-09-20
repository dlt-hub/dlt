{%- test expect_table_column_count_to_equal_other_table(model, compare_model) -%}
{%- if execute -%}
{%- set number_columns = (adapter.get_columns_in_relation(model) | length) -%}
{%- set compare_number_columns = (adapter.get_columns_in_relation(compare_model) | length) -%}
with test_data as (

    select
        {{ number_columns }} as number_columns,
        {{ compare_number_columns }} as compare_number_columns

)
select *
from test_data
where
    number_columns != compare_number_columns
{%- endif -%}
{%- endtest -%}
