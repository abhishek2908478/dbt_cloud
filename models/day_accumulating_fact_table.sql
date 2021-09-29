{{ config(materialized='incremental',incremental_strategy = 'merge')}}

with DAY_ACCUMULATING_FACT_TABLE AS (
    select L_ORDERKEY, L_PARTKEY,L_SUPPKEY, O_ORDERDATE, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE,
        DATEDIFF(day, O_ORDERDATE, L_SHIPDATE) READY_DAYS,
        DATEDIFF(day, L_SHIPDATE, L_RECEIPTDATE) SHIP_DAYS,    
        DATEDIFF(day, O_ORDERDATE, L_COMMITDATE) COMMIT_DAYS,
        DATEDIFF(day, O_ORDERDATE, L_RECEIPTDATE) TOTAL_DAYS,    
        DATEDIFF(day, L_COMMITDATE, L_RECEIPTDATE) DIFF_DAYS
    FROM  DEMO_DB.SOURCE_DATA.LINEITEM,  DEMO_DB.SOURCE_DATA.ORDERS
    WHERE L_ORDERKEY = O_ORDERKEY
)

SELECT * FROM DAY_ACCUMULATING_FACT_TABLE

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where L_RECEIPTDATE >= (select max(L_RECEIPTDATE) from {{ this }})
{% endif %}