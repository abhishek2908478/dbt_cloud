{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='KEY_ID')}}

with supplier_source_data as (

    select S_SUPPKEY, S_NAME,S_NATIONKEY,S_ACCTBAL
    from {{ ref('supplier_model_view') }}

),
supplier_max_datetime as (
    {% if is_incremental() %}

        select KEY_ID, S_SUPPKEY, S_NAME, S_NATIONKEY, S_ACCTBAL, CREATE_DATETIME
        from {{this}} ssc
        where CREATE_DATETIME = (select max(ssc1.CREATE_DATETIME) from {{this}} ssc1 where ssc1.S_SUPPKEY=ssc.S_SUPPKEY)

    {% else %}
        select 1
    {% endif %}
)

{% if is_incremental() %}

    select

    SUPPLIER_SCD4_PJN.nextval AS KEY_ID,
    sm.S_SUPPKEY, sm.S_NAME, sm.S_NATIONKEY, sm.S_ACCTBAL,
    CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS CREATE_DATETIME
    
    FROM supplier_source_data sm
        LEFT JOIN supplier_max_datetime ssc
            ON ssc.S_SUPPKEY = sm.S_SUPPKEY              
            AND ssc.S_ACCTBAL = sm.S_ACCTBAL
    WHERE ssc.S_SUPPKEY IS NULL

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.SUPPLIER_SCD4_PJN START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT SUPPLIER_SCD4_PJN.nextval AS KEY_ID ,S_SUPPKEY, S_NAME, S_NATIONKEY, S_ACCTBAL, CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS CREATE_DATETIME
    FROM SUPPLIER_SOURCE_DATA sm

{% endif %}