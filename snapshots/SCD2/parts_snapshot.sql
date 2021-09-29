{% snapshot dim_parts_scd2 %}

    {{
        config(
            strategy='check',
            check_cols=['p_retailprice', 'p_mfgr'],
            unique_key='p_partkey'
        )
    }}

    select p_partkey,p_name, p_mfgr, p_brand, p_retailprice from {{ ref('part_model_view') }}

{% endsnapshot %}