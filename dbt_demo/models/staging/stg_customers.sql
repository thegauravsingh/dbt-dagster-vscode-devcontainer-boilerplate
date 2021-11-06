with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        case
            when random() > 0.9 then 'active'
            else 'inactive'
        end as customer_status

    from source

)

select * from renamed
