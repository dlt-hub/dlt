with data_example as (

    select
        1 as idx,
        '2020-10-21' as date_col,
        cast(0 as {{ dbt.type_float() }}) as col_numeric_a

    union all

    select
        2 as idx,
        '2020-10-22' as date_col,
        1 as col_numeric_a

    union all

    select
        2 as idx,
        '2020-10-23' as date_col,
        2 as col_numeric_a

    union all

    select
        2 as idx,
        '2020-10-24' as date_col,
        1 as col_numeric_a

    union all

    select
        3 as idx,
        '2020-10-23' as date_col,
        0.5 as col_numeric_a
    union all

    select
        4 as idx,
        '2020-10-23' as date_col,
        0.5 as col_numeric_a

)
select
    *,
    sum(col_numeric_a) over (partition by idx order by date_col) as rolling_sum_increasing,
    sum(col_numeric_a) over (partition by idx order by date_col desc) as rolling_sum_decreasing
from
    data_example
