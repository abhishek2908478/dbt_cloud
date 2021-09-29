{% macro supplier() %}
    {% if is_incremental() %}

--   this filter will only be applied on an incremental run
    where s_suppkey > (select max(s_suppkey) from {{ this }})

    {% endif %}
{% endmacro %}