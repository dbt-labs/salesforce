{{ config(

  materialized = "table",
  dist = "opportunity_id",
  sort = "created_date"

) }}

with users as (

    select * from {{ref('sf_user')}}

),

history as (

    select * from {{ref('sf_opportunity_history')}}

),

opps as (

    select * from {{ref('sf_opportunity')}}

),

joined as (

    select

        history.opportunity_history_id as opp_hist_id,
        history.opportunity_id,
        history.created_by_id,
        history.stage_name,
        history.created_date,
        users.full_name as owner_name,
        opps.account_id

    from history
    left join users on history.created_by_id = users.user_id
    left join opps using (opportunity_id)

),

last_closed as (

    select
        *,
        case
            when created_date = max(created_date) over (partition by
            opportunity_id order by created_date rows between
            unbounded preceding and unbounded following)
            and stage_name ilike '%closed%'
                then 'last closed'
            when created_date = max(created_date) over (partition by
            opportunity_id order by created_date rows between
            unbounded preceding and unbounded following)
                then 'last'
            else 'not last'
        end as last_event_status
    from joined

),

final as (

    select
        *,
        case
            when last_event_status = 'last closed'
                then dateadd('day', 1, created_date)
            when last_event_status = 'last'
                then dateadd('day', 1, current_date)
            else lead(created_date) over (partition by opportunity_id order by
            created_date)
        end as active_to
    from last_closed

)

select * from final
