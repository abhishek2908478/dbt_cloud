
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false,unique_key='S_SUPPKEY')}}

with supplier as (

    select supply_incremental.nextval as KEY_ID, S_SUPPKEY,S_NAME,S_NATIONKEY,S_ACCTBAL
    from {{ ref('supplier_model_view') }}

),
final as (
      select KEY_ID ,S_SUPPKEY,S_NAME,S_NATIONKEY,S_ACCTBAL
      FROM supplier
)


select *
from final sm

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    where sm.S_SUPPKEY not in (select S_SUPPKEY from {{this}})
    or EXISTS  (select 1 from {{this}} ssc            
        where ssc.S_SUPPKEY = sm.S_SUPPKEY            
        AND (ssc.S_NAME != sm.S_NAME                 
        or ssc.S_NATIONKEY != sm.S_NATIONKEY               
        or ssc.S_ACCTBAL != sm.S_ACCTBAL))
{% endif %}