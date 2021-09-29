{{ config(materialized='incremental',incremental_strategy = 'merge', transient=false, unique_key='KEY_ID')}}

with part_source_data as (

    select P_PARTKEY, P_NAME,P_BRAND,P_RETAILPRICE
    from {{ ref('part_model_view') }}

)

{% if is_incremental() %}

    SELECT
    IFNULL(psc.KEY_ID, PART_SCD6.nextval) AS KEY_ID,
    IFNULL(pm.P_PARTKEY, psc.P_PARTKEY) AS P_PARTKEY,
    IFNULL(pm.P_NAME, psc.P_NAME) AS P_NAME,
    IFNULL(pm.P_BRAND, psc.P_BRAND) AS P_BRAND,

    CASE
        WHEN pm.P_PARTKEY IS NULL THEN pmab.P_RETAILPRICE
        ELSE pm.P_RETAILPRICE
    END AS P_RETAILPRICE,

    CASE
        WHEN psc.P_PARTKEY IS NULL THEN pm.P_RETAILPRICE
        ELSE psc.PREV_P_RETAILPRICE
    END AS PREV_P_RETAILPRICE,

    CASE
        WHEN psc.P_PARTKEY IS NULL THEN CURRENT_DATE()
        ELSE psc.START_DATE
    END AS START_DATE,

    CASE
        WHEN pm.P_PARTKEY IS NULL THEN CURRENT_DATE()
        ELSE NULL
    END AS EXPIRE_DATE,

    CASE
        WHEN pm.P_PARTKEY IS NULL THEN 'N'
        ELSE 'Y'
    END AS CURRENT_FLAG

    FROM PART_SOURCE_DATA pm
        FULL OUTER JOIN {{this}} psc
            ON pm.P_PARTKEY = psc.P_PARTKEY
            AND pm.P_RETAILPRICE = psc.P_RETAILPRICE
        LEFT JOIN PART_SOURCE_DATA pmab
            ON psc.P_PARTKEY = pmab.P_PARTKEY

    WHERE pm.P_PARTKEY IS NULL OR psc.P_PARTKEY IS NULL

{% else %}

    {%- call statement('reset_seq', fetch_result=True) -%}
        CREATE OR REPLACE SEQUENCE DEMO_DB.TARGET_MODELS.PART_SCD6 START = 1 INCREMENT = 1;
    {%- endcall -%}

    SELECT PART_SCD6.nextval AS KEY_ID ,P_PARTKEY, P_NAME, P_BRAND, P_RETAILPRICE, P_RETAILPRICE AS PREV_P_RETAILPRICE, CURRENT_DATE() AS START_DATE, NULL AS EXPIRE_DATE, 'Y' AS CURRENT_FLAG
    FROM PART_SOURCE_DATA pm

{% endif %}