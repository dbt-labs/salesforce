with source as (

    select * from {{var('schema')}}.user

),

renamed as (

    select

        id as user_id,

        -- keys
        accountid as account_id,

        -- personage
        firstname as first_name,
        middlename as middle_name,
        lastname as last_name,
        name as full_name,
        username as user_name,
        title,
        email,
        phone as work_phone,
        mobilephone as mobile_phone,

        -- metadata
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed
