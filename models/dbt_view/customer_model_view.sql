
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with customer_source_data as (

    select * from {{ source('ecommerce', 'customer') }}

),
final as (
      select C_CUSTKEY, upper(C_NAME) as C_NAME, C_NATIONKEY, C_ACCTBAL, C_PHONE, C_ADDRESS
      FROM customer_source_data
)

select *
from final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
