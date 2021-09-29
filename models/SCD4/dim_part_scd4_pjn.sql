{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='KEY_ID')}}

with part_source_data as (

    select P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE
    from {{ ref('part_model_view') }}

),
part_max_datetime as (
    {% if is_incremental() %}

        select KEY_ID, P_PARTKEY, P_NAME, P_BRAND, P_RETAILPRICE, CREATE_DATETIME
        from {{this}} psc
        where CREATE_DATETIME = (select max(psc1.CREATE_DATETIME) from {{this}} psc1 where psc1.P_PARTKEY=psc.P_PARTKEY)

    {% else %}
        select 1
    {% endif %}
)

{% if is_incremental() %}

    select

    PART_SCD4_PJN.nextval AS KEY_ID,
    pm.P_PARTKEY, pm.P_NAME, pm.P_BRAND, pm.P_RETAILPRICE,
    CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS CREATE_DATETIME
    
    FROM part_source_data pm
        LEFT JOIN part_max_datetime psc
            ON psc.P_PARTKEY = pm.P_PARTKEY              
            AND psc.P_RETAILPRICE = pm.P_RETAILPRICE
            AND psc.P_BRAND = pm.P_BRAND
    WHERE psc.P_PARTKEY IS NULL

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.PART_SCD4_PJN START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT PART_SCD4_PJN.nextval AS KEY_ID ,P_PARTKEY, P_NAME, P_BRAND, P_RETAILPRICE, CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS CREATE_DATETIME
    FROM PART_SOURCE_DATA pm

{% endif %}