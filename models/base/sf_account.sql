with source as (

    select * from {{ var('sf_account') }}

),

renamed as (

    select

        id as account_id,

        -- keys
        parentid as parent_id,
        ownerid as owner_id,

        -- logistics
        type as account_type,
        billingstreet as company_street,
        billingcity as company_city,
        billingstate as company_state,
        billingcountry as company_country,
        billingpostalcode as company_zipcode,

        -- details
        name as company_name,
        industry,
        description,
        numberofemployees as number_of_employees,

        -- metadata
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed
