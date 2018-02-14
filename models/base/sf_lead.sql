with source as (

    select * from {{ var('sf_lead') }}
    where isdeleted = false

),

renamed as (

    select

        id as lead_id,

        -- keys
        ownerid as owner_id,
        convertedopportunityid as opportunity_id,
        convertedaccountid as account_id,
        convertedcontactid as contact_id,

        -- conversion pipeline
        leadsource as lead_source,
        status,
        isconverted as is_converted,
        converteddate as converted_date,

        -- contact
        firstname as first_name,
        middlename as middle_name,
        lastname as last_name,
        name as full_name,
        title,
        email,
        phone as work_phone,
        mobilephone as mobile_phone,
        donotcall as can_call,

        -- outreach
        isunreadbyowner as is_unread_by_owner,

        -- company context
        company as company_name,
        city as company_city,
        website,
        numberofemployees as number_of_employees,

        -- metadata
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed
