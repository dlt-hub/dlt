{% test expect_column_values_to_be_in_set(model, column_name,
                                                   value_set,
                                                   quote_values=True,
                                                   row_condition=None
                                                   ) %}

with all_values as (

    select
        {{ column_name }} as value_field

    from {{ model }}
    {% if row_condition %}
    where {{ row_condition }}
    {% endif %}

),
set_values as (

    {% for value in value_set -%}
    select
        {% if quote_values -%}
        cast('{{ value }}' as {{ dbt.type_string() }})
        {%- else -%}
        {{ value }}
        {%- endif %} as value_field
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
validation_errors as (
    -- values from the model that are not in the set
    select
        v.value_field
    from
        all_values v
        left join
        set_values s on v.value_field = s.value_field
    where
        s.value_field is null

)

select *
from validation_errors

{% endtest %}
