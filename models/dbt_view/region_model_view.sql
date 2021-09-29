
{{ config(materialized='view') }}

with region as (
    select * from {{ source('ecommerce', 'region') }}
),
final as (
    select r_regionkey,r_name from region
)
select * from final