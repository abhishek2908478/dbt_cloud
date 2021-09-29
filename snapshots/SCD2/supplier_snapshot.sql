{% snapshot dim_supplier_scd2 %}

    {{
        config(
            strategy='check',
            check_cols=['s_address', 's_nationkey', 's_phone', 's_acctbal'],
            unique_key='s_suppkey'
        )
    }}

    select s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal from {{ ref('supplier_model_view') }}

{% endsnapshot %}