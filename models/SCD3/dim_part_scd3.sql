{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='P_PARTKEY')}}

with part_source_data as (

    select P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE
    from {{ ref('part_model_view') }}

)

{% if is_incremental() %}

    select

    IFNULL(psc.KEY_ID, PART_SCD3.nextval) AS KEY_ID,
    pm.P_PARTKEY, pm.P_NAME, pm.P_BRAND, pm.P_RETAILPRICE,
    psc.P_RETAILPRICE AS PREV_P_RETAILPRICE
    
    FROM part_source_data pm
        LEFT JOIN {{this}} psc
            ON psc.P_PARTKEY = pm.P_PARTKEY

    WHERE pm.P_PARTKEY NOT IN (SELECT P_PARTKEY FROM {{this}})
    OR EXISTS (SELECT 1 FROM {{this}} psc2            
            where psc2.P_PARTKEY = pm.P_PARTKEY            
            AND psc2.P_RETAILPRICE != pm.P_RETAILPRICE)

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.PART_SCD3 START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT PART_SCD3.nextval AS KEY_ID ,P_PARTKEY, P_NAME, P_BRAND, P_RETAILPRICE, NULL AS PREV_P_RETAILPRICE
    FROM PART_SOURCE_DATA pm

{% endif %}