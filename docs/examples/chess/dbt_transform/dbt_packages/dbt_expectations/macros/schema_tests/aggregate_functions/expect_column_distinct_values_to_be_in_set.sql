{% test expect_column_distinct_values_to_be_in_set(model,
                                                     column_name,
                                                     value_set,
                                                     quote_values=True,
                                                     row_condition=None
                                                     ) %}

with all_values as (

    select distinct
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
        '{{ value }}'
        {%- else -%}
        {{ value }}
        {%- endif %} as value_field
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),
unique_set_values as (

    select distinct value_field
    from
        set_values

),
validation_errors as (
    -- values from the model that are not in the set
    select
        v.value_field
    from
        all_values v
        left join
        unique_set_values s on v.value_field = s.value_field
    where
        s.value_field is null

)

select *
from validation_errors

{% endtest %}
