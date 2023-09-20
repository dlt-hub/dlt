{% macro get_select(model, expression, row_condition, group_by) -%}
    {{ adapter.dispatch('get_select', 'dbt_expectations') (model, expression, row_condition, group_by) }}
{%- endmacro %}

{%- macro default__get_select(model, expression, row_condition, group_by) %}
    select
        {% if group_by %}
        {% for g in group_by -%}
            {{ g }} as col_{{ loop.index }},
        {% endfor -%}
        {% endif %}
        {{ expression }} as expression
    from
        {{ model }}
    {%- if row_condition %}
    where
        {{ row_condition }}
    {% endif %}
    {% if group_by %}
    group by
        {% for g in group_by -%}
            {{ loop.index }}{% if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
{% endmacro -%}


{% test equal_expression(model, expression,
                                compare_model=None,
                                compare_expression=None,
                                group_by=None,
                                compare_group_by=None,
                                row_condition=None,
                                compare_row_condition=None,
                                tolerance=0.0,
                                tolerance_percent=None
                                ) -%}

    {{ adapter.dispatch('test_equal_expression', 'dbt_expectations') (
                                model,
                                expression,
                                compare_model,
                                compare_expression,
                                group_by,
                                compare_group_by,
                                row_condition,
                                compare_row_condition,
                                tolerance,
                                tolerance_percent) }}
{%- endtest %}

{%- macro default__test_equal_expression(
                                model,
                                expression,
                                compare_model,
                                compare_expression,
                                group_by,
                                compare_group_by,
                                row_condition,
                                compare_row_condition,
                                tolerance,
                                tolerance_percent) -%}

    {%- set compare_model = model if not compare_model else compare_model -%}
    {%- set compare_expression = expression if not compare_expression else compare_expression -%}
    {%- set compare_row_condition = row_condition if not compare_row_condition else compare_row_condition -%}
    {%- set compare_group_by = group_by if not compare_group_by else compare_group_by -%}

    {%- set n_cols = (group_by|length) if group_by else 0 %}
    with a as (
        {{ dbt_expectations.get_select(model, expression, row_condition, group_by) }}
    ),
    b as (
        {{ dbt_expectations.get_select(compare_model, compare_expression, compare_row_condition, compare_group_by) }}
    ),
    final as (

        select
            {% for i in range(1, n_cols + 1) -%}
            coalesce(a.col_{{ i }}, b.col_{{ i }}) as col_{{ i }},
            {% endfor %}
            a.expression,
            b.expression as compare_expression,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))/
                nullif(a.expression * 1.0, 0) as expression_difference_percent
        from
        {% if n_cols > 0 %}
            a
            full outer join
            b on
            {% for i in range(1, n_cols + 1) -%}
                a.col_{{ i }} = b.col_{{ i }} {% if not loop.last %}and{% endif %}
            {% endfor -%}
        {% else %}
            a cross join b
        {% endif %}
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        {% if tolerance_percent %}
        expression_difference_percent > {{ tolerance_percent }}
        {% else %}
        expression_difference > {{ tolerance }}
        {% endif %}
{%- endmacro -%}
