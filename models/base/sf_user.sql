with source as (

    select * from {{ var('sf_user') }}

),

renamed as (

    select

        id as user_id,

        profile_firstname as first_name,
        profile_lastname as last_name,
        emails,

        company_companyid as company_id,
        company_role,
        company_position,

        username,
        roles as user_roles,

        createdat as created_at,
        updatedat as updated_at

    from source

)

select * from renamed
