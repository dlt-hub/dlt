{{
    config(
        materialized='table'
    )
}}

{% if should_full_refresh() %}
    -- take all loads when full refresh
    SELECT load_id, schema_name, schema_version_hash FROM {{ source('dlt', '_dlt_loads') }}
    -- TODO: the status value must be configurable so we can chain packages
    WHERE status = 0
{% else %}
    -- take only loads with status = 0 and no other records
    SELECT load_id, schema_name, schema_version_hash FROM {{ source('dlt', '_dlt_loads') }}
    GROUP BY load_id, schema_name, schema_version_hash
    -- note that it is a hack - we make sure no other statuses exist
    HAVING SUM(status) = 0
{% endif %}