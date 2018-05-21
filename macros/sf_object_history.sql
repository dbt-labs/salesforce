{% macro sf_object_history(object,custom=false) -%}

{%- set custom = false -%}
{%- if "__" in object -%}
    {%- set custom = true -%}
{%- endif -%}

{%- set object_name = (object | replace("history","") | replace("field","") | replace("_","")) -%}

{%- set parent -%}
    {%- if custom == true -%} parent
    {%- else -%} {{object_name}}
    {%- endif -%}
{%- endset -%}

    with source as (

    	select * from {{var('schema')}}.{{object}}

    ),

    renamed as (

    	select
        
    		id as {{object_name}}_history_id,
    		{{parent}}id as {{parent}}_id,
    		createdbyid as created_by_user_id,
    		createddate as created_date,
    		field,
        
        {% if custom == false %}
            oldvalue as old_value,
            newvalue as new_value,
    	{% endif %}
        
        	isdeleted as is_deleted,
    		_sdc_batched_at as meta_timestamp

    	from source

    ),

    ordered as (

        select *,

            row_number() over (partition by {{object_name}}_history_id, created_date order by meta_timestamp desc)
                as meta_index

        from renamed

    ),

    latest as (

        select * from ordered
        where meta_index = 1

    )

    select * from latest

{% endmacro %}