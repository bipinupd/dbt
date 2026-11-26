{% macro bigquery__create_table_as(temporary, relation, compiled_code, language='sql', replace=true) -%}
  {%- if language == 'sql' -%}
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set raw_cluster_by = config.get('cluster_by', none) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
    {%- if partition_config.time_ingestion_partitioning -%}
    {%- set columns = get_columns_with_types_in_query_sql(sql) -%}
    {%- set table_dest_columns_csv = columns_without_partition_fields_csv(partition_config, columns) -%}
    {%- set columns = '(' ~ table_dest_columns_csv ~ ')' -%}
    {%- endif -%}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
    {#-- Never replace a snapshot's target table. The default snapshot
         materialization creates it only on the first build, when the relation cache says it does not
         exist, so a inconsistent cache now errors instead of silently dropping a snapshot. Later runs build a
         temporary staging table, thus we should keep `replace`. `model` is not defined in all the existing calling contexts. --#}
    {%- set is_snapshot = model is defined and model.resource_type == 'snapshot' -%}
    {%- set is_snapshot_target = is_snapshot and not temporary -%}
    {%- set replace = replace and not is_snapshot_target -%}

    {{ sql_header if sql_header is not none }}

    create {% if replace %}or replace {% endif %}table {{ relation }}
      {%- set contract_config = config.get('contract') -%}
      {%- if contract_config.enforced -%}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {{ get_table_columns_and_constraints() }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
      {% else %}
        {#-- cannot do contracts at the same time as time ingestion partitioning -#}
        {{ columns }}
      {% endif %}
    {{ partition_by(partition_config) }}
    {{ cluster_by(raw_cluster_by) }}

    {% if catalog_relation.table_format == 'iceberg' and not temporary and not catalog_relation.lakehouse_catalog %}
    {%- if catalog_relation.connection_id -%} with connection `{{ catalog_relation.connection_id }}`
    {%- else -%} with connection default
    {%- endif -%}
    {% endif %}
    {{ bigquery_table_options(config, model, temporary) }}

    {#-- PARTITION BY cannot be used with the AS query_statement clause.
         https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression
    -#}
    {%- if not partition_config.time_ingestion_partitioning %}
    as (
      {{ compiled_code }}
    );
    {%- endif %}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}

    {#-- when a user wants to change the schema of an existing relation, they must intentionally drop the table in the dataset --#}
    {%- set old_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
    {%- if (old_relation.is_table and (should_full_refresh())) -%}
      {% do adapter.drop_relation(relation) %}
    {%- endif -%}
    {%- set submission_method = config.get("submission_method", "serverless") -%}
    {%- if submission_method in ("serverless", "cluster") -%}
      {{ py_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
    {%- elif submission_method == "bigframes" -%}
      {{ bigframes_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
    {%- else -%}
      {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported dataframe syntax, it got %s" % submission_method) %} {%- endif -%}
  {%- else -%}
    {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}

{%- endmacro -%}

{% macro bq_create_table_as_with_replace(temporary, relation, compiled_code, language='sql', replace=true) %}
  {% if replace %}
    {{ return(create_table_as(temporary, relation, compiled_code, language)) }}
  {% else %}
    {{ return(bigquery__create_table_as(temporary, relation, compiled_code, language, replace=false)) }}
  {% endif %}
{% endmacro %}

{% macro bigquery__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }}
  {{ bigquery_view_options(config, model) }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  as {{ sql }};

{% endmacro %}

{# DIVERGENCE: in core, drop_schema is implemented via the googleapi inside the adapter.drop_schema method.
   Under dbt-core (1.x), short-circuit to the adapter call; under Fusion (2.x) the SQL path is preserved
   byte-for-byte to keep xdbc replay hashes stable. #}
{% macro bigquery__drop_schema(relation) -%}
  {%- if not dbt_version.startswith('2.') -%}{{ return(adapter.drop_schema(relation)) }}{%- endif -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {%- endcall -%}
{% endmacro %}


{% macro bigquery__list_relations_without_caching(schema_relation) -%}
  {{ return(adapter.list_relations_without_caching(schema_relation)) }}
{%- endmacro %}


{# DIVERGENCE: in core, list_schemas is implemented via the googleapi inside the adapter.list_schemas method.
   Under dbt-core (1.x), short-circuit to the adapter call; under Fusion (2.x) the SQL path is preserved
   byte-for-byte to keep xdbc replay hashes stable. #}
{% macro bigquery__list_schemas(database) -%}
  {%- if not dbt_version.startswith('2.') -%}{{ return(adapter.list_schemas(database)) }}{%- endif -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct schema_name from {{ database }}.INFORMATION_SCHEMA.SCHEMATA;
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro bigquery__check_schema_exists(information_schema, schema) %}
  {{ return(adapter.check_schema_exists(information_schema.database, schema)) }}
{% endmacro %}

{#-- Handle both relation and column level documentation #}
{% macro bigquery__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do alter_relation_comment(relation, model.description) %}
  {% endif %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {#-- BigQuery: build STRUCT-aware valid column names (leaves + intermediate paths) --#}
    {% set existing_columns = adapter.get_columns_in_relation(relation) %}
    {% set valid_names = [] %}
    {% set record_names = [] %}
    {% for col in existing_columns %}
      {% for flat_col in col.flatten() %}
        {% set parts = flat_col.name.split(".") %}
        {% for i in range(1, parts | length + 1) %}
          {% set prefix = parts[:i] | join(".") %}
          {% if prefix not in valid_names %}
            {% do valid_names.append(prefix) %}
          {% endif %}
          {% if i < (parts | length) %}
            {% set prefix_lower = prefix | lower %}
            {% if prefix_lower not in record_names %}
              {% do record_names.append(prefix_lower) %}
            {% endif %}
          {% endif %}
        {% endfor %}
      {% endfor %}
    {% endfor %}
    {% set filtered_columns = validate_doc_columns(relation, model.columns, valid_names) %}
    {# DIVERGENCE BEGIN: On Fusion (2.x), persist BigQuery column-level data_governance_tags
       via batched ALTER TABLE ... ALTER COLUMN DDL until the BigQuery ADBC driver adds
       REST dataGovernanceTagsInfo support. Validate upfront before mutating any column. #}
    {% if dbt_version.startswith('2.') %}
      {% set dg_tags_sql = bigquery__get_alter_column_data_governance_tags_sql(relation, filtered_columns, record_names) %}
      {% set rollback_clear_cols = {} %}
      {% set remaining_dg_cols = {} %}
      {% for col_name in filtered_columns %}
        {% set col = filtered_columns[col_name] %}
        {% set raw_tags = col['data_governance_tags'] if ('data_governance_tags' in col) else none %}
        {% if raw_tags is defined and raw_tags is not none and raw_tags is mapping and (raw_tags | length) == 0 %}
          {% set ns = namespace(has_pt=false) %}
          {% if ('policy_tags' in col) and col['policy_tags'] is not none %}
            {% set raw_pt = col['policy_tags'] %}
            {% if raw_pt is string %}
              {% if (raw_pt | trim | length) > 0 %}{% set ns.has_pt = true %}{% endif %}
            {% else %}
              {% for pt in raw_pt %}
                {% if pt is string and (pt | trim | length) > 0 %}{% set ns.has_pt = true %}{% endif %}
              {% endfor %}
            {% endif %}
          {% endif %}
          {% if ns.has_pt %}
            {% do rollback_clear_cols.update({col_name: col}) %}
          {% else %}
            {% do remaining_dg_cols.update({col_name: col}) %}
          {% endif %}
        {% else %}
          {% do remaining_dg_cols.update({col_name: col}) %}
        {% endif %}
      {% endfor %}
      {% if (rollback_clear_cols | length) > 0 %}
        {% set pre_clear_sql = bigquery__get_alter_column_data_governance_tags_sql(relation, rollback_clear_cols, record_names) %}
        {% if pre_clear_sql %}
          {% do run_query(pre_clear_sql) %}
        {% endif %}
        {% do alter_column_comment(relation, filtered_columns) %}
        {% set post_sql = bigquery__get_alter_column_data_governance_tags_sql(relation, remaining_dg_cols, record_names) %}
        {% if post_sql %}
          {% do run_query(post_sql) %}
        {% endif %}
      {% else %}
        {% do alter_column_comment(relation, filtered_columns) %}
        {% if dg_tags_sql %}
          {% do run_query(dg_tags_sql) %}
        {% endif %}
      {% endif %}
    {% else %}
      {% do alter_column_comment(relation, filtered_columns) %}
    {% endif %}
    {# DIVERGENCE END #}
  {% endif %}
{% endmacro %}

{% macro bigquery__alter_relation_comment(relation, relation_comment) -%}
  {# DIVERGENCE BEGIN: In BigQuery a DDL already sets the description, so Fusion may skip the
     redundant update via the `bigquery_noop_alter_relation_comment` behavior flag — a
     Fusion-only behavior surface. Under dbt-core (1.x), call update_table_description
     unconditionally (matches v1 dbt-bigquery behavior); Fusion path preserved unchanged. #}
  {%- if not dbt_version.startswith('2.') -%}
    {% do adapter.update_table_description(relation.database, relation.schema, relation.identifier, relation_comment) %}
    {{ return('') }}
  {%- endif -%}
  {%- set noop = (relation.type in ['table', 'view'] and not is_incremental()) or adapter.behavior.bigquery_noop_alter_relation_comment.no_warn -%}
  {%- if not noop -%}
    {% do adapter.update_table_description(relation.database, relation.schema, relation.identifier, relation_comment) %}
  {%- endif -%}
  {# DIVERGENCE END #}
{% endmacro %}

{% macro bigquery__alter_column_comment(relation, column_dict) -%}
  {% do adapter.update_columns(relation, column_dict) %}
{% endmacro %}

{# DIVERGENCE BEGIN: Fusion-only helpers emitting batched ALTER COLUMN DDL for data_governance_tags. #}
{% macro bigquery__alter_column_data_governance_tags(relation, column_dict, record_columns=none) -%}
  {% set sql = bigquery__get_alter_column_data_governance_tags_sql(relation, column_dict, record_columns) %}
  {% if sql %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}

{% macro bigquery__get_alter_column_data_governance_tags_sql(relation, column_dict, record_columns=none) -%}
  {%- set record_lower = [] -%}
  {%- if record_columns is not none -%}
    {%- for rec_name in record_columns -%}
      {%- do record_lower.append(rec_name | lower) -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set actions = [] -%}
  {%- for col_name in column_dict -%}
    {%- set col = column_dict[col_name] -%}
    {%- set raw_tags = col['data_governance_tags'] if ('data_governance_tags' in col) else none -%}
    {%- if raw_tags is defined and raw_tags is not none -%}
      {%- if raw_tags is not mapping -%}
        {{ exceptions.raise_compiler_error(
          "Invalid `data_governance_tags` for column '" ~ col_name ~ "': expected a mapping of {'<parent_id>/<tag_key>': '<tag_value>'}."
        ) }}
      {%- endif -%}
      {%- set has_entries = (raw_tags | length) > 0 -%}
      {%- if relation.type != 'table' -%}
        {%- if has_entries -%}
          {{ exceptions.raise_compiler_error(
            "Column '" ~ col_name ~ "' specifies `data_governance_tags`, which BigQuery only supports on table columns (not views, materialized views, snapshots, or external tables)."
          ) }}
        {%- endif -%}
      {%- else -%}
        {%- set dtype_raw = (col['data_type'] if ('data_type' in col and col['data_type'] is not none) else '') | lower -%}
        {%- set dtype_norm = dtype_raw.split() | join('') -%}
        {%- set is_record_dtype = (dtype_norm in ['record', 'struct', 'array<struct>', 'array<record>']) or ('struct<' in dtype_norm) or ('record<' in dtype_norm) -%}
        {%- set is_record = is_record_dtype or ((col_name | lower) in record_lower) -%}
        {%- if is_record -%}
          {%- if has_entries -%}
            {{ exceptions.raise_compiler_error(
              "Column '" ~ col_name ~ "' is a RECORD/STRUCT column and cannot have `data_governance_tags` attached directly. Attach `data_governance_tags` to leaf fields instead."
            ) }}
          {%- endif -%}
        {%- elif '.' in col_name -%}
          {{ exceptions.raise_compiler_error(
            "Column '" ~ col_name ~ "' specifies `data_governance_tags` on a nested STRUCT field. BigQuery ALTER COLUMN DDL does not accept dotted paths; use REST tables.patch (or update_columns in dbt-bigquery 1.x) for nested leaves."
          ) }}
        {%- else -%}
          {%- if has_entries and ('policy_tags' in col) and col['policy_tags'] is not none -%}
            {%- set raw_pt = col['policy_tags'] -%}
            {%- if raw_pt is string -%}
              {%- if (raw_pt | trim | length) > 0 -%}
                {{ exceptions.raise_compiler_error(
                  "Column '" ~ col_name ~ "' cannot specify both `policy_tags` and `data_governance_tags`. BigQuery does not allow both on the same column."
                ) }}
              {%- endif -%}
            {%- else -%}
              {%- for pt in raw_pt -%}
                {%- if pt is string and (pt | trim | length) > 0 -%}
                  {{ exceptions.raise_compiler_error(
                    "Column '" ~ col_name ~ "' cannot specify both `policy_tags` and `data_governance_tags`. BigQuery does not allow both on the same column."
                  ) }}
                {%- endif -%}
              {%- endfor -%}
            {%- endif -%}
          {%- endif -%}
          {%- set tuples = [] -%}
          {%- for tag_key, tag_value in raw_tags.items() -%}
            {%- if tag_key is not string -%}
              {{ exceptions.raise_compiler_error(
                "Invalid `data_governance_tags` key for column '" ~ col_name ~ "': expected string in '<parent_id>/<tag_key>' format."
              ) }}
            {%- endif -%}
            {%- set parts = tag_key.split('/') -%}
            {%- set has_ws = (tag_key.split() | join('')) != tag_key -%}
            {%- if (parts | length) != 2 or (parts[0] | length) == 0 or (parts[1] | length) == 0 or has_ws -%}
              {{ exceptions.raise_compiler_error(
                "Invalid `data_governance_tags` key '" ~ tag_key ~ "' for column '" ~ col_name ~ "': expected '<parent_id>/<tag_key>' (e.g. 'my-project/data_class' or '123456789/data_class') with no whitespace."
              ) }}
            {%- endif -%}
            {%- if tag_value is not string or (tag_value | trim | length) == 0 or (tag_value | trim) != tag_value -%}
              {{ exceptions.raise_compiler_error(
                "Invalid `data_governance_tags` value for key '" ~ tag_key ~ "' on column '" ~ col_name ~ "': expected a non-empty tag value short name string without leading or trailing whitespace."
              ) }}
            {%- endif -%}
            {%- if '/' in tag_value -%}
              {{ exceptions.raise_compiler_error(
                "Invalid `data_governance_tags` value '" ~ tag_value ~ "' for key '" ~ tag_key ~ "' on column '" ~ col_name ~ "': BigQuery expects the leaf tag value short name (e.g. 'email'), not a hierarchical path."
              ) }}
            {%- endif -%}
            {%- set key_literal = '"' ~ (tag_key | replace('\\', '\\\\') | replace('"', '\\"') | replace('\n', '\\n') | replace('\r', '\\r')) ~ '"' -%}
            {%- set value_literal = '"' ~ (tag_value | replace('\\', '\\\\') | replace('"', '\\"') | replace('\n', '\\n') | replace('\r', '\\r')) ~ '"' -%}
            {%- do tuples.append('(' ~ key_literal ~ ', ' ~ value_literal ~ ')') -%}
          {%- endfor -%}
          {%- set quoted_name = adapter.quote(col_name) -%}
          {%- do actions.append('alter column ' ~ quoted_name ~ ' set options (data_governance_tags=[' ~ (tuples | join(', ')) ~ '])') -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if (actions | length) == 0 -%}
    {{ return('') }}
  {%- else -%}
    {{ return('alter ' ~ relation.type ~ ' ' ~ relation ~ '\n' ~ (actions | join(',\n'))) }}
  {%- endif -%}
{%- endmacro %}
{# DIVERGENCE END #}

{% macro bigquery__alter_relation_add_columns(relation, add_columns) %}

  {% set sql -%}

     alter {{ relation.type }} {{ relation }}
        {% for column in add_columns %}
          add column {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
        {% endfor %}

  {%- endset -%}

  {{ return(run_query(sql)) }}

{% endmacro %}

{% macro bigquery__alter_relation_drop_columns(relation, drop_columns) %}

  {% set sql -%}

     alter {{ relation.type }} {{ relation }}

        {% for column in drop_columns %}
          drop column {{ column.name }}{{ ',' if not loop.last }}
        {% endfor %}

  {%- endset -%}

  {{ return(run_query(sql)) }}

{% endmacro %}


{# DIVERGENCE: FIXME:
   Fusion is missing the adapter.alter_table_add_remove_columns method.
   Only uncomment this when this is implemented.
{% macro bigquery__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% do adapter.alter_table_add_remove_columns(relation, add_columns, remove_columns) %}
{% endmacro %}
#}


{% macro bigquery__alter_column_type(relation, column_name, new_column_type) -%}
  {#-- Changing a column's data type using a query requires you to scan the entire table.
    The query charges can be significant if the table is very large.

    https://cloud.google.com/bigquery/docs/manually-changing-schemas#changing_a_columns_data_type
  #}
  {% set relation_columns = get_columns_in_relation(relation) %}

  {% set sql %}
    select
      {%- for col in relation_columns -%}
        {% if col.column == column_name %}
          CAST({{ col.quoted }} AS {{ new_column_type }}) AS {{ col.quoted }}
        {%- else %}
          {{ col.quoted }}
        {%- endif %}
        {%- if not loop.last %},{% endif -%}
      {%- endfor %}
    from {{ relation }}
  {% endset %}

  {% call statement('alter_column_type') %}
    {{ create_table_as(False, relation, sql)}}
  {%- endcall %}

{% endmacro %}


{% macro bigquery__test_unique(model, column_name) %}

with dbt_test__target as (

  select {{ column_name }} as unique_field
  from {{ model }}
  where {{ column_name }} is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1

{% endmacro %}

{% macro bigquery__upload_file(local_file_path, database, table_schema, table_name) %}

  {{ log("kwargs: " ~ kwargs) }}

  {% do adapter.upload_file(local_file_path, database, table_schema, table_name, kwargs=kwargs) %}

{% endmacro %}

{% macro bigquery__make_relation_with_suffix(base_relation, suffix, dstring) %}
    {% if dstring %}
      {% set dt = modules.datetime.datetime.now() %}
      {% set dtstring = dt.strftime("%H%M%S%f") %}
      {% set suffix = suffix ~ dtstring %}
    {% endif %}
    {% set suffix_length = suffix|length %}
    {% set relation_max_name_length = 1024 %}  {# BigQuery limit #}
    {% if suffix_length > relation_max_name_length %}
        {% do exceptions.raise_compiler_error('Relation suffix is too long (' ~ suffix_length ~ ' characters). Maximum length is ' ~ relation_max_name_length ~ ' characters.') %}
    {% endif %}
    {% set identifier = base_relation.identifier[:relation_max_name_length - suffix_length] ~ suffix %}

    {{ return(base_relation.incorporate(path={"identifier": identifier })) }}

{% endmacro %}

{% macro bigquery__make_temp_relation(base_relation, suffix) %}
    {% set temp_relation = bigquery__make_relation_with_suffix(base_relation, suffix, dstring=True) %}
    {{ return(temp_relation) }}
{% endmacro %}

{% macro bigquery__make_intermediate_relation(base_relation, suffix) %}
    {{ return(bigquery__make_relation_with_suffix(base_relation, suffix, dstring=False)) }}
{% endmacro %}

{% macro bigquery__make_backup_relation(base_relation, backup_relation_type, suffix) %}
    {% set backup_relation = bigquery__make_relation_with_suffix(base_relation, suffix, dstring=False) %}
    {{ return(backup_relation.incorporate(type=backup_relation_type)) }}
{% endmacro %}


{# DIVERGENCE: in core, create_schema dispatches to the default global macro.
   Under dbt-core (1.x), short-circuit to the adapter call; under Fusion (2.x) the SQL path is preserved
   byte-for-byte to keep xdbc replay hashes stable. #}
{% macro bigquery__create_schema(relation) -%}
  {%- if not dbt_version.startswith('2.') -%}{{ return(adapter.create_schema(relation)) }}{%- endif -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}
{% endmacro %}


{# DIVERGENCE #}
{% macro bigquery__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
        select count(*)
	from {{ information_schema }}.SCHEMATA
        where upper(schema_name) = upper('{{ schema }}')
            and upper(catalog_name) = upper('{{ information_schema.database }}')
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}
