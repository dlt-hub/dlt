{%- macro _get_metric_expression(metric_column, take_logs) -%}

{%- if take_logs %}
{%- set expr = "nullif(" ~ metric_column ~ ", 0)" -%}
coalesce({{ dbt_expectations.log_natural(expr) }}, 0)
{%- else -%}
coalesce({{ metric_column }}, 0)
{%- endif %}

{%- endmacro -%}

{% test expect_column_values_to_be_within_n_moving_stdevs(model,
                                  column_name,
                                  date_column_name,
                                  group_by=None,
                                  period='day',
                                  lookback_periods=1,
                                  trend_periods=7,
                                  test_periods=14,
                                  sigma_threshold=3,
                                  sigma_threshold_upper=None,
                                  sigma_threshold_lower=None,
                                  take_diffs=true,
                                  take_logs=true
                                ) -%}
    {{ adapter.dispatch('test_expect_column_values_to_be_within_n_moving_stdevs', 'dbt_expectations') (model,
                                  column_name,
                                  date_column_name,
                                  group_by,
                                  period,
                                  lookback_periods,
                                  trend_periods,
                                  test_periods,
                                  sigma_threshold,
                                  sigma_threshold_upper,
                                  sigma_threshold_lower,
                                  take_diffs,
                                  take_logs
                                ) }}
{%- endtest %}

{% macro default__test_expect_column_values_to_be_within_n_moving_stdevs(model,
                                  column_name,
                                  date_column_name,
                                  group_by,
                                  period,
                                  lookback_periods,
                                  trend_periods,
                                  test_periods,
                                  sigma_threshold,
                                  sigma_threshold_upper,
                                  sigma_threshold_lower,
                                  take_diffs,
                                  take_logs
                                ) %}

{%- set sigma_threshold_upper = sigma_threshold_upper if sigma_threshold_upper else sigma_threshold -%}
{%- set sigma_threshold_lower = sigma_threshold_lower if sigma_threshold_lower else -1 * sigma_threshold -%}
{%- set partition_by = "partition by " ~ (group_by | join(",")) if group_by -%}
{%- set group_by_length = (group_by | length ) if group_by else 0 -%}

with metric_values as (

    with grouped_metric_values as (

        select
            {{ dbt.date_trunc(period, date_column_name) }} as metric_period,
            {{ group_by | join(",") ~ "," if group_by }}
            sum({{ column_name }}) as agg_metric_value
        from
            {{ model }}
        {{  dbt_expectations.group_by(1 + group_by_length) }}

    )
    {%- if take_diffs %}
    , grouped_metric_values_with_priors as (

        select
            *,
            lag(agg_metric_value, {{ lookback_periods }}) over(
                {{ partition_by }}
                order by metric_period) as prior_agg_metric_value
    from
        grouped_metric_values d

    )
    select
        *,
        {{ dbt_expectations._get_metric_expression("agg_metric_value", take_logs) }}
        -
        {{ dbt_expectations._get_metric_expression("prior_agg_metric_value", take_logs) }}
        as metric_test_value
    from
        grouped_metric_values_with_priors d

    {%- else %}

    select
        *,
        {{ dbt_expectations._get_metric_expression("agg_metric_value", take_logs) }}
        as metric_test_value
    from
        grouped_metric_values

    {%- endif %}

),
metric_moving_calcs as (

    select
        *,
        avg(metric_test_value)
            over({{ partition_by }}
                    order by metric_period rows
                    between {{ trend_periods }} preceding and 1 preceding) as metric_test_rolling_average,
        stddev(metric_test_value)
            over({{ partition_by }}
                    order by metric_period rows
                    between {{ trend_periods }} preceding and 1 preceding) as metric_test_rolling_stddev
    from
        metric_values

),
metric_sigma as (

    select
        *,
        (metric_test_value - metric_test_rolling_average) as metric_test_delta,
        (metric_test_value - metric_test_rolling_average)/
            nullif(metric_test_rolling_stddev, 0) as metric_test_sigma
    from
        metric_moving_calcs

)
select
    *
from
    metric_sigma
where

    metric_period >= cast(
            {{ dbt.dateadd(period, -test_periods, dbt.date_trunc(period, dbt_date.now())) }}
            as {{ dbt_expectations.type_timestamp() }})
    and
    metric_period < {{ dbt.date_trunc(period, dbt_date.now()) }}
    and

    not (
        metric_test_sigma >= {{ sigma_threshold_lower }} and
        metric_test_sigma <= {{ sigma_threshold_upper }}
    )
{%- endmacro -%}
