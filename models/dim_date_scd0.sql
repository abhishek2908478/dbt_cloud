{{ config(materialized='table', transient='true',
    pre_hook="create or replace sequence date_sequence_5 start = 1 increment = 1;") }}

with fifty_year_dates as (

    {{ dbt_date.get_date_dimension('1992-01-01', '2041-12-31') }}

)

select *
from fifty_year_dates