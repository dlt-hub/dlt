{% test expect_column_values_to_match_like_pattern(model, column_name,
                                                    like_pattern,
                                                    row_condition=None
                                                    ) %}

{% set expression = dbt_expectations._get_like_pattern_expression(column_name, like_pattern, positive=True) %}

{{ dbt_expectations.expression_is_true(model,
                                        expression=expression,
                                        group_by_columns=None,
                                        row_condition=row_condition
                                        )
                                        }}

{% endtest %}
