--the output of this model is a day per opportunity history that the opp was active
{{
    config(
        materialized='table',
        sort='date_day',
        dist='opportunity_id'
    )
}}

with opp_history as (

  select distinct
    date_trunc('day', created_date)::date as start_date,
    date_trunc('day', active_to)::date as end_date,
    opportunity_id,
    sf_account_id,
    uv_account_id,
    csm_name,
    record_type,
    record_type_id
  from {{ref('sf_opportunity_history_joined')}}

),

days as (

/*  select * from {{ref('all_days')}}
  where date_day <= date_trunc('day', current_date) */

  dbt_utils.date_spine(datepart="day",
    start_date="to_date('01/01/2016', 'mm/dd/yyyy')",
    end_date="dateadd(week, 1, current_date)")

),

opp_days as (
--this creates the final output of one row per day the stage was active
    select
        days.date_day,
        date_trunc('month', days.date_day)::date as date_month,
        opp_history.*
    from days
      inner join opp_history
        on days.date_day >= opp_history.start_date
        and days.date_day < opp_history.end_date

)

select * from opp_days
