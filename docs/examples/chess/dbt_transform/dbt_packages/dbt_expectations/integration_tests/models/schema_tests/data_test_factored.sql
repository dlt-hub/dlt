select
    1 as factor,
    *
from
    {{ ref("data_test") }}

union all

select
    2 as factor,
    *
from
    {{ ref("data_test") }}
