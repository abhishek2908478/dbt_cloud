{% macro macro2() %}
    {% if is_incremental() %}
    -- Macro 2
        where s_suppkey > (select max(s_suppkey) from {{ this }})
    {% endif %}
{% endmacro %}