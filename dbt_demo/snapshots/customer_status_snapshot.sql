{% snapshot customer_status_snapshot %}

{{
   config(
       target_schema='snapshots',
       unique_key='customer_id',

       strategy='check',
       check_cols=['customer_status']
   )
}}

select
    customer_id,
    customer_status
from
    {{ ref('stg_customers') }}

{% endsnapshot %}