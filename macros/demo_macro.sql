{% macro macro1() %}
    {% if is_incremental() %}
    -- Macro 1
        where s_suppkey > (select max(s_suppkey) from {{ this }})
    {% endif %}
{% endmacro %}