{% snapshot dim_customer_scd2 %}

    {{
        config(
            strategy='check',
            check_cols=['c_address', 'c_nationkey', 'c_phone', 'c_acctbal'],
            unique_key='c_custkey'
        )
    }}

    select C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL, C_PHONE, C_ADDRESS from {{ ref('customer_model_view') }}

{% endsnapshot %}