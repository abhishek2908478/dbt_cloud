{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='S_SUPPKEY')}}

with supplier_source_data as (

    select S_SUPPKEY,S_NAME,S_NATIONKEY,S_ACCTBAL
    from {{ ref('supplier_model_view') }}

)

{% if is_incremental() %}

    select

    IFNULL(ssc.KEY_ID, SUPPLIER_SCD3.nextval) AS KEY_ID,
    sm.S_SUPPKEY, sm.S_NAME, sm.S_NATIONKEY, sm.S_ACCTBAL,
    ssc.S_ACCTBAL AS PREV_S_ACCTBAL
    
    FROM supplier_source_data sm
        LEFT JOIN {{this}} ssc
            ON ssc.S_SUPPKEY = sm.S_SUPPKEY

    WHERE sm.S_SUPPKEY NOT IN (SELECT S_SUPPKEY FROM {{this}})
    OR EXISTS (SELECT 1 FROM {{this}} ssc2            
            where ssc2.S_SUPPKEY = sm.S_SUPPKEY            
            AND ssc2.S_ACCTBAL != sm.S_ACCTBAL)

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.SUPPLIER_SCD3 START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT SUPPLIER_SCD3.nextval AS KEY_ID ,S_SUPPKEY, S_NAME, S_NATIONKEY, S_ACCTBAL, NULL AS PREV_S_ACCTBAL
    FROM SUPPLIER_SOURCE_DATA sm

{% endif %}