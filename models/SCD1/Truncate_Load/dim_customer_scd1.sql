
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='table', transient=false,
    pre_hook="create or replace sequence seq_1 start = 1 increment = 1;") }}

with customer_source_data as (

    select seq_1.nextval as KEY_ID ,C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL from {{ ref('customer_model_view') }}

),
final as (
      select KEY_ID ,C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL
      FROM customer_source_data
)


select *
from final



/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
