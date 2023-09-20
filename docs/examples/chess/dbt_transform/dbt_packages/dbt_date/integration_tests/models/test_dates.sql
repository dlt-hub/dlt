{{
    config(
        materialized = 'table'
    )
}}
{{ dbt_date_integration_tests.get_test_dates() }}
