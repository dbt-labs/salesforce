with source as (

    select * from {{ var('sf_contact') }}

),

renamed as (

    select

        id as contact_id,

        -- keys
        ownerid as owner_id,
        accountid as account_id,

        -- personage
        firstname as first_name,
        middlename as middle_name,
        lastname as last_name,
        name as full_name,
        title,
        email,
        phone as work_phone,
        mobilephone as mobile_phone,

        -- metadata
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed
