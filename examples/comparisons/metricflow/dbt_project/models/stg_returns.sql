{{ config(materialized='view') }}
select * from {{ source('raw', 'returns') }}
