select
    customer_id,
    count(*) as number_status_change
from {{ ref('customer_status_snapshot') }}
group by 1
order by 2 desc