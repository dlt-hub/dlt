select
    1 as idx,
    '2020-10-21' as date_col,
    cast(0 as {{ dbt.type_float() }}) as col_numeric_a,
    cast(1 as {{ dbt.type_float() }}) as col_numeric_b,
    'a' as col_string_a,
    'b' as col_string_b,
    cast(null as {{ dbt.type_string() }}) as col_null,
    cast(null as {{ dbt.type_string() }}) as col_null_2

union all

select
    2 as idx,
    '2020-10-22' as date_col,
    1 as col_numeric_a,
    0 as col_numeric_b,
    'b' as col_string_a,
    'ab' as col_string_b,
    null as col_null,
    null as col_null_2

union all

select
    3 as idx,
    '2020-10-23' as date_col,
    0.5 as col_numeric_a,
    0.5 as col_numeric_b,
    'c' as col_string_a,
    'abc' as col_string_b,
    null as col_null,
    null as col_null_2

union all

select
    4 as idx,
    '2020-10-23' as date_col,
    0.5 as col_numeric_a,
    0.5 as col_numeric_b,
    'c' as col_string_a,
    'abcd' as col_string_b,
    null as col_null,
    null as col_null_2
