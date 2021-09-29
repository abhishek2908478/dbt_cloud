
{{ config(materialized='view') }}

with supplier as (
    select * from {{ source('ecommerce', 'supplier') }}
),
final as (
    select s_suppkey, upper(s_name) as s_name, s_address, s_nationkey, s_phone, s_acctbal from supplier
)
select * from final