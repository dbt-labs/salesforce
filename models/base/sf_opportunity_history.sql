with source as (

    select * from {{ var('sf_opportunity_history') }}
    where isdeleted = false

),

renamed as (

    select

        id as opportunity_history_id,

        -- keys
        opportunityid as opportunity_id,
        createdbyid as created_by_id,

        -- pipeline
        probability,
        stagename as stage_name,
        forecastcategory as forecast_category,
        amount,
        expectedrevenue as expected_revenue,

        -- metadata
        createddate as created_date


    from source

)

select * from renamed
