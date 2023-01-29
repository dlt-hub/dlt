-- never do a full refresh or you drop the original loads info
{{
    config(
        materialized='incremental',
        schema="event",
        full_refresh = false
    )
}}

select load_id, schema_name, 1 as status, {{ current_timestamp() }} as inserted_at from {{ ref('load_ids') }}
    WHERE load_id NOT IN (
        SELECT load_id FROM {{ source('dlt', '_dlt_loads') }} WHERE status = 1)
