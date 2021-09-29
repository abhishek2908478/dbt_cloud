
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='P_PARTKEY')}}

with part as (

    select part_incremental.nextval as KEY_ID ,P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE
    from {{ ref('part_model_view') }}

),
final as (
      select KEY_ID ,P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE
      FROM part
)


select *
from final cp

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where cp.P_PARTKEY not in (select P_PARTKEY from {{this}})
    or EXISTS  (select 1 from {{this}} psc            
        where psc.P_PARTKEY = cp.P_PARTKEY            
        AND (psc.P_NAME != cp.P_NAME                 
        or psc.P_BRAND != cp.P_BRAND                
        or psc.P_RETAILPRICE != cp.P_RETAILPRICE))
{% endif %}