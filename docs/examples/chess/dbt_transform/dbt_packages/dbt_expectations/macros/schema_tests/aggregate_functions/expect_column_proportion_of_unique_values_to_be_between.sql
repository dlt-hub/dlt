{% test expect_column_proportion_of_unique_values_to_be_between(model, column_name,
                                                            min_value=None,
                                                            max_value=None,
                                                            group_by=None,
                                                            row_condition=None,
                                                            strictly=False
                                                            ) %}
{% set expression %}
count(distinct {{ column_name }})*1.0/count({{ column_name }})
{% endset %}
{{ dbt_expectations.expression_between(model,
                                        expression=expression,
                                        min_value=min_value,
                                        max_value=max_value,
                                        group_by_columns=group_by,
                                        row_condition=row_condition,
                                        strictly=strictly
                                        ) }}

{% endtest %}
