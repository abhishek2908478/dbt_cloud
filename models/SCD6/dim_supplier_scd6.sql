{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='KEY_ID')}}

with supplier_source_data as (

    select  S_SUPPKEY, S_NAME, S_NATIONKEY, S_ACCTBAL from {{ ref('supplier_model_view') }}

)

{% if is_incremental() %}

    SELECT
    IFNULL(ssc.KEY_ID, SUPPLIER_SCD6.nextval) AS KEY_ID,
    IFNULL(sm.S_SUPPKEY, ssc.S_SUPPKEY) AS S_SUPPKEY,
    IFNULL(sm.S_NAME, ssc.S_NAME) AS S_NAME,
    IFNULL(sm.S_NATIONKEY, ssc.S_NATIONKEY) AS S_NATIONKEY,

    CASE
        WHEN sm.S_SUPPKEY IS NULL THEN smab.S_ACCTBAL
        ELSE sm.S_ACCTBAL
    END AS S_ACCTBAL,

    CASE
        WHEN ssc.S_SUPPKEY IS NULL THEN sm.S_ACCTBAL
        ELSE ssc.PREV_S_ACCTBAL
    END AS PREV_S_ACCTBAL,

    CASE
        WHEN ssc.S_SUPPKEY IS NULL THEN CURRENT_DATE()
        ELSE ssc.START_DATE
    END AS START_DATE,

    CASE
        WHEN sm.S_SUPPKEY IS NULL THEN CURRENT_DATE()
        ELSE NULL
    END AS EXPIRE_DATE,

    CASE
        WHEN sm.S_SUPPKEY IS NULL THEN 'N'
        ELSE 'Y'
    END AS CURRENT_FLAG

    FROM SUPPLIER_SOURCE_DATA sm
        FULL OUTER JOIN {{this}} ssc
            ON sm.S_SUPPKEY = ssc.S_SUPPKEY
            AND sm.S_ACCTBAL = ssc.S_ACCTBAL
        LEFT JOIN SUPPLIER_SOURCE_DATA smab
            ON ssc.S_SUPPKEY = smab.S_SUPPKEY

    WHERE sm.S_SUPPKEY IS NULL OR ssc.S_SUPPKEY IS NULL

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.SUPPLIER_SCD6 START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT SUPPLIER_SCD6.nextval AS KEY_ID ,S_SUPPKEY, S_NAME, S_NATIONKEY, S_ACCTBAL, S_ACCTBAL AS PREV_S_ACCTBAL, CURRENT_DATE() AS START_DATE, NULL AS EXPIRE_DATE, 'Y' AS CURRENT_FLAG
    FROM SUPPLIER_SOURCE_DATA sm

{% endif %}