{% test expect_column_values_to_be_unique(model, column_name, row_condition=None) %}
{{ dbt_expectations.test_expect_compound_columns_to_be_unique(model, [column_name], row_condition=row_condition) }}
{% endtest %}
