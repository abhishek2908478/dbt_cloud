
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='table', transient=false,
    pre_hook="create or replace sequence date_sequence_4 start = 1 increment = 1;") }}

with orders_source_data as (

    select DISTINCT(O_ORDERDATE) from {{ ref('orders_model_view') }}

),
final as (
      select date_sequence_4.nextval as KEY_ID ,
      O_ORDERDATE AS D_DATE, 
      date_part('year', O_ORDERDATE)::int AS D_YEAR,
      {{ dbt_date.month_name('O_ORDERDATE', short=false) }} AS D_MONTH, 
      {{ dbt_date.day_of_month('O_ORDERDATE') }} AS D_DAY_OF_MONTH,
      {{ dbt_date.day_name('O_ORDERDATE', short=false) }} AS D_WEEKDAY,
      {{ dbt_date.week_of_year('O_ORDERDATE') }} AS D_WEEK
      FROM orders_source_data
)

select *
from final



