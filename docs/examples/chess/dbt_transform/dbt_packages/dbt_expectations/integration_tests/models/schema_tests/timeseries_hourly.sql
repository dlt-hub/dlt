{% set end_date = modules.datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) %}
{% set start_date = (end_date - modules.datetime.timedelta(days=10)) %}

{{ dbt_date.get_base_dates(
        start_date=start_date,
        end_date=end_date,
        datepart="hour"
    )
}}
