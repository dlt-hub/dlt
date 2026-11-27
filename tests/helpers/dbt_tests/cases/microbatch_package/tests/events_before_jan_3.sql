{{ config(severity='warn') }}

select id from {{ ref('events_microbatch') }} where created_at < '2024-01-03'
