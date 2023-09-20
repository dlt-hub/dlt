{% test expect_column_values_to_match_like_pattern_list(model, column_name,
                                                    like_pattern_list,
                                                    match_on="any",
                                                    row_condition=None
                                                    ) %}

{% set expression %}
    {% for like_pattern in like_pattern_list %}
    {{ dbt_expectations._get_like_pattern_expression(column_name, like_pattern, positive=True) }}
    {%- if not loop.last %}
    {{ " and " if match_on == "all" else " or "}}
    {% endif -%}
    {% endfor %}
{% endset %}

{{ dbt_expectations.expression_is_true(model,
                                        expression=expression,
                                        group_by_columns=None,
                                        row_condition=row_condition
                                        )
                                        }}

{% endtest %}
