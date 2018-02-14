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
    account_id,
    owner_name,
    stage_name
  from {{ref('sf_opportunity_history_joined')}}

),

days as (

      {{ dbt_utils.date_spine(datepart="day",
        start_date="to_date('{{ var('first_record') }}', 'mm/dd/yyyy')",
        end_date="dateadd(week, 1, current_date)") }}

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

select
    {{ dbt_utils.surrogate_key('date_day','opportunity_id') }} as opp_daily_id,
    *
from opp_days
