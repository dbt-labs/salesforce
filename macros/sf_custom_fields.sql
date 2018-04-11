{% macro sf_custom_fields(sf_table) -%}

    {%- if sf_table.name -%}
        {%- set schema_name, table_name = sf_table.schema, sf_table.table -%}
    {%- else -%}
        {%- set schema_name, table_name = (sf_table | string).split(".") -%}
    {%- endif -%}

    {%- set custom_cols = [] %}
    {%- set cols = adapter.get_columns_in_table(schema_name, table_name) -%}

        {% for col in cols -%}

            {% if '__c' in col.column %}
                {% set _ = custom_cols.append(col.column) %}
            {%- endif %}

        {%- endfor -%}

        {%- for col in custom_cols %}

            {%- set new_col_name -%}

                {{ col.column | replace("__c", "") }}

            {%- endset -%}

            "{{ col }}" as "{{new_col_name}}"{% if not loop.last %},{%- endif -%}

        {%- endfor %}

{%- endmacro %}
