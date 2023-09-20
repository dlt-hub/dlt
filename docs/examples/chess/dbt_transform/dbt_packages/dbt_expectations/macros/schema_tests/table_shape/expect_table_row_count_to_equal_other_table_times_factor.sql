{%- test expect_table_row_count_to_equal_other_table_times_factor(model,
                                                                    compare_model,
                                                                    factor,
                                                                    group_by=None,
                                                                    compare_group_by=None,
                                                                    row_condition=None,
                                                                    compare_row_condition=None
                                            ) -%}
    {{ adapter.dispatch('test_expect_table_row_count_to_equal_other_table_times_factor',
                        'dbt_expectations') (model,
                                                compare_model,
                                                factor,
                                                group_by,
                                                compare_group_by,
                                                row_condition,
                                                compare_row_condition
                                            ) }}
{% endtest %}

{%- macro default__test_expect_table_row_count_to_equal_other_table_times_factor(model,
                                                                    compare_model,
                                                                    factor,
                                                                    group_by,
                                                                    compare_group_by,
                                                                    row_condition,
                                                                    compare_row_condition
                                                                    ) -%}

{{ dbt_expectations.test_expect_table_row_count_to_equal_other_table(model,
    compare_model,
    group_by=group_by,
    compare_group_by=compare_group_by,
    factor=factor,
    row_condition=row_condition,
    compare_row_condition=compare_row_condition
) }}
{%- endmacro -%}
