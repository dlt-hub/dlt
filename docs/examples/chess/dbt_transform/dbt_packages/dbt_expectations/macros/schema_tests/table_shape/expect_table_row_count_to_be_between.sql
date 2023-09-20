{%- test expect_table_row_count_to_be_between(model,
                                                min_value=None,
                                                max_value=None,
                                                group_by=None,
                                                row_condition=None,
                                                strictly=False
                                            ) -%}
    {{ adapter.dispatch('test_expect_table_row_count_to_be_between',
                        'dbt_expectations') (model,
                                                min_value,
                                                max_value,
                                                group_by,
                                                row_condition,
                                                strictly
                                                ) }}
{% endtest %}

{%- macro default__test_expect_table_row_count_to_be_between(model,
                                                min_value,
                                                max_value,
                                                group_by,
                                                row_condition,
                                                strictly
                                                ) -%}
{% set expression %}
count(*)
{% endset %}
{{ dbt_expectations.expression_between(model,
    expression=expression,
    min_value=min_value,
    max_value=max_value,
    group_by_columns=group_by,
    row_condition=row_condition,
    strictly=strictly
    ) }}
{%- endmacro -%}
