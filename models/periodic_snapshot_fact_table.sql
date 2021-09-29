{{ 
	config(
	materialized='incremental',
	transient=false, 
	incremental_strategy = 'merge',
	unique_key = 'date'
	)
}}

{% if execute %}
	{% set current_seq_val = run_query('select periodic_facttable_sequence.nextval').columns[0].values()[0] %}
	{# log(current_seq_val, info=True) #}
{% endif %}

-- with SELECT_FACT_TABLE_SCD1 AS (
-- 	select dateadd('day', {{ current_seq_val }}, dateadd('year', -27, current_date)) AS "DATE", 
--  		count(ORDER_KEY) as "NUMBER OF ORDERS", 
--  		count(distinct(CUST_KEY)) as "NUMBER OF CUSTOMERS", 
--  		sum(PRICE) as "TOTAL PRICE PAID BY CUSTOMERS" 
-- 	from {{ref('cost_fact_table')}}
-- 	{% if not is_incremental() %}
-- 		where ORDER_DATE= dateadd('YEAR',-27,CURRENT_DATE)
-- 	{% endif %}
-- )

select ORDER_DATE AS "DATE", 
 		count(ORDER_KEY) as "NUMBER OF ORDERS", 
 		count(distinct(CUST_KEY)) as "NUMBER OF CUSTOMERS", 
 		sum(PRICE) as "TOTAL PRICE PAID BY CUSTOMERS" 
	from {{ref('cost_fact_table')}}

-- select * from SELECT_FACT_TABLE_SCD1 

{% if is_incremental() %}
 -- where ORDER_DATE > (select MAX(DATE) from {{ this }})
	where ORDER_DATE between dateadd('YEAR',-27,CURRENT_DATE + {{ current_seq_val }} - 2)
							and dateadd('year',-27,CURRENT_DATE + {{ current_seq_val }})
{% else %}
	where ORDER_DATE <= dateadd('YEAR',-27,CURRENT_DATE + 1)
{% endif %}

group by date