
{{ config(materialized='view') }}

with part as (
    select * from {{ source('ecommerce', 'part') }}
),
final as (
    select p_partkey, upper(p_name) as p_name, p_mfgr, p_brand, p_retailprice from part
)
select * from final