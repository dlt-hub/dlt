
{% test expect_column_pair_values_to_be_equal(model,
                                                        column_A,
                                                        column_B,
                                                        row_condition=None
                                                        ) %}

{% set operator = "=" %}
{% set expression = column_A ~ " " ~ operator ~ " " ~ column_B %}

{{ dbt_expectations.expression_is_true(model,
                                        expression=expression,
                                        group_by_columns=None,
                                        row_condition=row_condition
                                        )
                                        }}

{% endtest %}
