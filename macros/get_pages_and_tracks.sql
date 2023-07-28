{% macro get_pages_and_tracks(table_names) -%}
    {% if execute %}
        {% set query %}
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM RUDDER_EVENTS.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME IN ('TRACKS')
        {% endset %}
        
        {{ log(query, info=True) }}
        {% set results = run_query(query) %}
        {{ log("Running get_tables: " ~ results, info=True) }}
        
        {% if results is none or results|length == 0 %}
            {% set error_log = "No tables found matching the specified criteria." %}
            {{ exceptions.raise_compiler_error(error_log) }}
        {% endif %}
        
        {% set relations = [] %}
        
        {% for row in results %}
            {% set relation = adapter.get_relation(database=target.database, schema=row['TABLE_SCHEMA'], identifier=row['TABLE_NAME']) %}
            
            {% if relation is not none %}
                {% do relations.append(relation) %}
            {% else %}
                {% set error_log = "No relation found for database " ~ target.database ~ ", schema " ~ row['TABLE_SCHEMA'] ~ ", and table " ~ row['TABLE_NAME'] %}
                {{ exceptions.raise_compiler_error(error_log) }}
            {% endif %}
        {% endfor %}
        
        {{ return(relations) }}
    {% endif %}
{% endmacro %}
