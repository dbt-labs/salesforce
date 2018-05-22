{%- set history_fields = [
    'probability',
    'stage_name',
    'forecast_category',
    'amount',
    'expected_revenue'
    ] -%}

{%- set custom_fields = var('opportunity_history_custom_fields') -%}

with opphist as (

    select
    
        opportunity_history_id,
        opportunity_id,
        created_by_id,
        created_date,
        is_deleted,
        meta_timestamp,
        meta_index,
        probability,
        stage_name,
        forecast_category,
        amount,
        expected_revenue
        
        {{ "," if (custom_fields|length) > 0 }}
    
        {% for custom_field in custom_fields -%}
            null as {{custom_field}}{{"," if not loop.last}}
        {%- endfor %}
    
    from {{ref('sf_opportunity_history')}}

),

fieldhist as (
    
    select
    
        opportunity_history_id,
        opportunity_id,
        created_by_user_id as created_by_id,
        created_date,
        is_deleted,
        meta_timestamp,
        meta_index,

        {% for history_field in history_fields %}
            null as {{history_field}}{{"," if not loop.last}}
        {% endfor %}{{ "," if (custom_fields|length) > 0 }}
        
        {% for custom_field in custom_fields -%}
            case when field = '{{custom_field}}' then new_value else null end as {{custom_field}}{{"," if not loop.last}}
        {%- endfor %}
    
    from {{ref('sf_opportunity_field_history')}}
    where field in ( '{{custom_fields|join("', '")}}' )
    
),

unioned as (
    
    select * from opphist
    union all
    select * from fieldhist
    
),

filled as (
    
    select
    
        opportunity_history_id,
        opportunity_id,
        created_by_id,
        created_date,
        is_deleted,
        meta_timestamp,
        
        {%- set all_fields = history_fields + custom_fields -%}
        {% for field in all_fields %}
        
            coalesce({{field}},lag({{field}}) ignore nulls over (partition by opportunity_id 
                order by created_date)) as {{field}}{{"," if not loop.last}}
                
        {% endfor %}
    
    from unioned
    
)

select * from filled