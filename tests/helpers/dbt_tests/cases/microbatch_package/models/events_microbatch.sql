{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key='id',
        event_time='created_at',
        begin='2024-01-01',
        batch_size='day',
    )
}}

select id, created_at, {{ dlt_load_id() }} as _dlt_load_id
from {{ source('dlt', 'events') }}
