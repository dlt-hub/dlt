{{
    config(
        materialized = "table"
    )
}}
{{ dbt_date.get_date_dimension('2015-01-01', '2022-12-31') }}
