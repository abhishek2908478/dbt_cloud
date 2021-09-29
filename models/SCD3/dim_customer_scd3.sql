{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='C_CUSTKEY')}}

with customer_source_data as (

    select C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL
    from {{ ref('customer_model_view') }}

)

{% if is_incremental() %}

    select

    IFNULL(csc.KEY_ID, CUSTOMER_SCD3.nextval) AS KEY_ID,
    cm.C_CUSTKEY, cm.C_NAME, cm.C_NATIONKEY, cm.C_ACCTBAL,
    csc.C_ACCTBAL AS PREV_C_ACCTBAL
    
    FROM customer_source_data cm
        LEFT JOIN {{this}} csc
            ON csc.C_CUSTKEY = cm.C_CUSTKEY

    WHERE CM.C_CUSTKEY NOT IN (SELECT C_CUSTKEY FROM {{this}})
    OR EXISTS (SELECT 1 FROM {{this}} csc2            
            where csc2.C_CUSTKEY = cm.C_CUSTKEY            
            AND csc2.C_ACCTBAL != cm.C_ACCTBAL)

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.CUSTOMER_SCD3 START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT CUSTOMER_SCD3.nextval AS KEY_ID ,C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL, NULL AS PREV_C_ACCTBAL
    FROM CUSTOMER_SOURCE_DATA CM

{% endif %}