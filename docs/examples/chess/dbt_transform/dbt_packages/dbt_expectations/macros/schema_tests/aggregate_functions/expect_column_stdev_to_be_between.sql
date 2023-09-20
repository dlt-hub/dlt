{% test expect_column_stdev_to_be_between(model, column_name,
                                                    min_value=None,
                                                    max_value=None,
                                                    group_by=None,
                                                    row_condition=None,
                                                    strictly=False
                                                    ) -%}
    {{ adapter.dispatch('test_expect_column_stdev_to_be_between', 'dbt_expectations') (
                                                    model, column_name,
                                                    min_value,
                                                    max_value,
                                                    group_by,
                                                    row_condition,
                                                    strictly
                                                    ) }}
{%- endtest %}

{% macro default__test_expect_column_stdev_to_be_between(
                                                    model, column_name,
                                                    min_value,
                                                    max_value,
                                                    group_by,
                                                    row_condition,
                                                    strictly
                                                    ) %}

{% set expression %}
stddev({{ column_name }})
{% endset %}
{{ dbt_expectations.expression_between(model,
                                        expression=expression,
                                        min_value=min_value,
                                        max_value=max_value,
                                        group_by_columns=group_by,
                                        row_condition=row_condition,
                                        strictly=strictly
                                        ) }}
{% endmacro %}
