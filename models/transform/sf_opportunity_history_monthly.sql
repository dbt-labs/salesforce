--the output of this model is row per month per opportunity that the opportunity
--was active. it also provides the CSM assigned at the beginning of the month

{{
    config(
        materialized='table',
        sort='date_month',
        dist='uv_account_id'
    )
}}

with opp_history as (

  select * from {{ref('sf_opportunity_history_daily')}}

),

opp_months as (

--this rolls the daily values up monthly and sees who was assigned the opp
--at the beginning of the month

    select distinct

        date_month,
        opportunity_id,
        account_id,
        first_value(owner_name ignore nulls) over (partition by opportunity_id,
            date_month order by date_day rows between unbounded preceding and
            unbounded following) as owner_name,
        first_value(stage_name ignore nulls) over (partition by opportunity_id,
            date_month order by date_day rows between unbounded preceding and
            unbounded following) as stage_name

    from opp_history

)

select
    {{ dbt_utils.surrogate_key('date_month','opportunity_id') }} as opp_monthly_id,
    *
from opp_months
