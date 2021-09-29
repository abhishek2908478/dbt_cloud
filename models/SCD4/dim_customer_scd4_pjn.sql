{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='KEY_ID')}}

with customer_source_data as (

    select C_CUSTKEY, C_NAME,C_NATIONKEY,C_ACCTBAL
    from {{ ref('customer_model_view') }}

),
customer_max_datetime as (
    {% if is_incremental() %}

        select KEY_ID, C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL, CREATE_DATETIME
        from {{this}} csc
        where CREATE_DATETIME = (select max(csc1.CREATE_DATETIME) from {{this}} csc1 where csc1.C_CUSTKEY=csc.C_CUSTKEY)

    {% else %}
        select 1
    {% endif %}
)

{% if is_incremental() %}

    select

    CUSTOMER_SCD4_PJN.nextval AS KEY_ID,
    cm.C_CUSTKEY, cm.C_NAME, cm.C_NATIONKEY, cm.C_ACCTBAL,
    CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS CREATE_DATETIME
    
    FROM customer_source_data cm
        LEFT JOIN customer_max_datetime csc
            ON csc.C_CUSTKEY = cm.C_CUSTKEY              
            AND csc.C_ACCTBAL = cm.C_ACCTBAL
    WHERE csc.C_CUSTKEY IS NULL

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.CUSTOMER_SCD4_PJN START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT CUSTOMER_SCD4_PJN.nextval AS KEY_ID ,C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL, CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS CREATE_DATETIME
    FROM CUSTOMER_SOURCE_DATA cm

{% endif %}