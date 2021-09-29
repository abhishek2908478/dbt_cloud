{% macro call_procedure() %}

{% set result = run_query("call sample_proc()") %}
{{ log(result,info=True) }}

{% endmacro %}]