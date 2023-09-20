{% test expect_column_pair_values_to_be_in_set(model,
                                                        column_A,
                                                        column_B,
                                                        value_pairs_set,
                                                        row_condition=None
                                                        ) %}

{% set expression %}
{% for pair in value_pairs_set %}
{%- if (pair | length) == 2 %}
({{ column_A }} = {{ pair[0] }} and {{ column_B }} = {{ pair[1] }}){% if not loop.last %} or {% endif %}
{% else %}
{{ exceptions.raise_compiler_error(
        "`value_pairs_set` argument for expect_column_pair_values_to_be_in_set test cannot have more than 2 item per element.
        Got: '" ~ pair ~ "'.'"
    ) }}
{% endif %}
{% endfor %}
{% endset %}
{{ dbt_expectations.expression_is_true(model,
                                        expression=expression,
                                        group_by_columns=None,
                                        row_condition=row_condition
                                        )
                                        }}

{% endtest %}
