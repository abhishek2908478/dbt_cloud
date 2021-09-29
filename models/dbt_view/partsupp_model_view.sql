
{{ config(materialized='view') }}

with partsupp as (
    select * from {{ source('ecommerce', 'partsupp') }}
),
final as (
    select ps_partkey,ps_suppkey,ps_availqty,ps_supplycost from partsupp
)
select * from final