
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='C_CUSTKEY')}}

with customer_source_data as (

    select customer_incremental.nextval as KEY_ID, C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL
    from {{ ref('customer_model_view') }}

),
final as (
      select  KEY_ID ,C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL
      FROM customer_source_data
)


select *
from final cm

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where cm.C_CUSTKEY not in (select C_CUSTKEY from {{this}})
    or EXISTS  (select 1 from {{this}} csc            
        where csc.C_CUSTKEY = cm.C_CUSTKEY            
        AND (csc.C_NAME != cm.C_NAME                 
        or csc.C_NATIONKEY != cm.C_NATIONKEY                
        or csc.C_ACCTBAL != cm.C_ACCTBAL))
{% endif %}