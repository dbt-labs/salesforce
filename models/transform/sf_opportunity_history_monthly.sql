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
        sf_account_id,
        uv_account_id,
        first_value(csm_name ignore nulls) over (partition by opportunity_id,
            date_month order by date_day rows between unbounded preceding and
            unbounded following) as csm_name,
        first_value(record_type ignore nulls) over (partition by opportunity_id,
            date_month order by date_day rows between unbounded preceding and
            unbounded following) as record_type,
        first_value(record_type_id ignore nulls) over (partition by opportunity_id,
            date_month order by date_day rows between unbounded preceding and
            unbounded following) as record_type_id
    from opp_history


)

select * from opp_months
