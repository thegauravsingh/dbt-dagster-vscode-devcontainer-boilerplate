{{
  config(
    tags = 'monthly',
    )
}}

select 
	to_char(order_date, 'YYYY-MM') as month_order,
	count(*) as number_orders
from {{ ref('orders') }}
group by 1