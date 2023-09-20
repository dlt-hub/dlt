{%- test expect_column_values_to_be_of_type(model, column_name, column_type) -%}
{{ dbt_expectations.test_expect_column_values_to_be_in_type_list(model, column_name, [column_type]) }}
{%- endtest -%}

