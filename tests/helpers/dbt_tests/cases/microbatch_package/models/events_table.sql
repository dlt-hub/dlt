{{ config(materialized='table') }}

select id, created_at from {{ source('dlt', 'events') }}
