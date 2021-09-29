
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with nation_source_data as (

    select * from {{ source('ecommerce', 'nation') }}  

),
final as (
       select N_NATIONKEY, N_NAME, N_REGIONKEY
    FROM nation_source_data
)

select *
from final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
