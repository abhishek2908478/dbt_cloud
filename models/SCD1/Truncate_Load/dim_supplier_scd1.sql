
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='table', transient=false,
    pre_hook="create or replace sequence seq_2 start = 1 increment = 1;") }}

with supplier as (

    select seq_2.nextval as KEY_ID ,S_SUPPKEY,S_NAME,S_NATIONKEY,S_ACCTBAL from {{ ref('supplier_model_view') }}

),
final as (
      select KEY_ID ,S_SUPPKEY,S_NAME,S_NATIONKEY,S_ACCTBAL from supplier
)


select *
from final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
