{% test expect_column_values_to_have_consistent_casing(model, column_name, display_inconsistent_columns=False) %}

with test_data as (

    select
        distinct {{ column_name }} as distinct_values
    from
        {{ model }}

 ),
 {% if display_inconsistent_columns %}
 validation_errors as (

    select
        lower(distinct_values) as inconsistent_columns,
        count(distinct_values) as set_count_case_insensitive
    from
        test_data
    group by 1
    having
        count(distinct_values) > 1

 )
 select * from validation_errors
 {% else %}
 validation_errors as (

    select
        count(1) as set_count,
        count(distinct lower(distinct_values)) as set_count_case_insensitive
    from
        test_data

 )
 select *
 from
    validation_errors
 where
    set_count != set_count_case_insensitive
 {% endif %}
 {%- endtest -%}
