{% test expect_column_value_lengths_to_equal(model, column_name,
                                                    value,
                                                    row_condition=None
                                                    ) %}

{% set expression = dbt.length(column_name) ~ " = " ~ value %}

{{ dbt_expectations.expression_is_true(model,
                                        expression=expression,
                                        group_by_columns=None,
                                        row_condition=row_condition
                                        )
                                        }}

{% endtest %}
