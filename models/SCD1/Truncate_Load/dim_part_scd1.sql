
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='table', transient=false,
    pre_hook="create or replace sequence seq_3 start = 1 increment = 1;") }}

with part as (

    select seq_3.nextval as KEY_ID, P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE from {{ ref('part_model_view') }}

),
final as (
      select KEY_ID, P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE
      FROM part
)


select *
from final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
