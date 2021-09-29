{{ config(materialized='incremental', transient=false, incremental_strategy = 'merge')}}

with SELECT_FACT_TABLE_SCD1  AS (
    select D.D_DATE AS ORDER_DATE, O.O_ORDERKEY AS ORDER_KEY, C.C_CUSTKEY AS CUST_KEY, P.P_PARTKEY AS PART_KEY, S.S_SUPPKEY AS SUPP_KEY, ROUND(L.L_QUANTITY, 0) AS QUANTITY, ROUND((L.L_QUANTITY * P.P_RETAILPRICE * (1-L_DISCOUNT) * (1+L_TAX)), 2) AS PRICE
    FROM
        (SELECT O_ORDERKEY, O_ORDERDATE, O_CUSTKEY
        FROM {{ref('orders_model_view')}} ) AS O
    INNER JOIN
        (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, l_extendedprice * (1-l_discount) * (1+l_tax) AS L_PRICE
        FROM {{ref('lineitem_model_view')}} ) AS L 
    ON O.O_ORDERKEY = L.L_ORDERKEY    
    INNER JOIN
        (SELECT C_CUSTKEY 
        FROM {{ref('dim_customer_scd1')}} ) AS C
    ON O.O_CUSTKEY = C.C_CUSTKEY    
    INNER JOIN
        (SELECT S_SUPPKEY
        FROM {{ref('dim_supplier_scd1')}} ) AS S
    ON L.L_SUPPKEY = S.S_SUPPKEY    
    INNER JOIN
        (SELECT P_PARTKEY, P_RETAILPRICE
        FROM {{ref('dim_part_scd1')}} ) AS P
    ON L.L_PARTKEY = P.P_PARTKEY    
    INNER JOIN
        (SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST
        FROM {{ref('partsupp_model_view')}} ) AS PS
    ON L.L_SUPPKEY = PS.PS_SUPPKEY 
    AND L.L_PARTKEY = PS.PS_PARTKEY
    INNER JOIN
        (SELECT D_DATE
        FROM {{ref('dim_date_scd1')}} ) AS D
    ON O.O_ORDERDATE = D.D_DATE
)

select * from SELECT_FACT_TABLE_SCD1 cm

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where ORDER_KEY NOT IN (select ORDER_KEY from {{ this }})
    -- or EXISTS (
    --     select 1 from {{this}} csc 
    --     where cm.ORDER_DATE = (select max(ORDER_DATE) from {{ this }}) 
    --     AND (
    --         csc.CUST_KEY != cm.CUST_KEY 
    --         or csc.PART_KEY != cm.PART_KEY 
    --         or csc.SUPP_KEY != cm.SUPP_KEY
    --     )
    -- )
{% endif %}