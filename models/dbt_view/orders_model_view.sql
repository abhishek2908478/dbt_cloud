
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with orders_source_data as (

    select * from {{ source('ecommerce', 'orders') }}  
    
),
final as (
      select O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE
    FROM orders_source_data
)

select *
from final



/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
