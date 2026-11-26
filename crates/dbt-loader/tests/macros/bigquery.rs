use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::flags::Flags;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config};

fn build_harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build")
}

#[test]
fn snapshot_check_cols_matches_existing_column_case_insensitively() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_macro(
            "dbt_bigquery",
            "bigquery__get_columns_in_query",
            "{% macro bigquery__get_columns_in_query(select_sql) %}{{ return(['dice_change_hash']) }}{% endmacro %}",
        )
        .build()
        .expect("harness should build");

    harness
        .mock()
        .on("get_relation", |_| Ok(Value::from("snapshot_target")));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from_serialize(vec![BTreeMap::from([(
            "name",
            "DICE_CHANGE_HASH",
        )])]))
    });
    harness.mock().on("quote", |args| {
        Ok(Value::from(format!(
            "`{}`",
            args[0].as_str().expect("column name")
        )))
    });

    let node = BTreeMap::from([
        ("database", "test-db"),
        ("schema", "test_schema"),
        ("alias", "member_snapshot"),
        ("compiled_code", "select DICE_CHANGE_HASH from source"),
    ]);
    let rendered = harness
        .render(
            "{% set added, cols = snapshot_check_all_get_existing_columns(node, true, ['dice_change_hash']) %}{{ 'added' if added else 'existing' }}|{{ cols | join(',') }}",
            BTreeMap::from([("node", Value::from_serialize(node))]),
        )
        .expect("snapshot check should render");

    assert_eq!(rendered.trim(), "existing|`dice_change_hash`");
}

fn base_relation_ctx(harness: &MacroTestHarness) -> BTreeMap<String, Value> {
    let relation = harness.relation(
        "test-db",
        "test_schema",
        "member_snapshot",
        Some(RelationType::Table),
    );
    BTreeMap::from([
        (
            "base_relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        // `make_temp_relation` (dbt-adapters/macros/adapters/relation.sql) reads
        // `model.batch` before dispatching; a real dbt `model` always has this key
        // (None for non-microbatch models), so mirror that shape here.
        (
            "model".to_string(),
            Value::from_serialize(BTreeMap::from([("batch", Value::from(()))])),
        ),
    ])
}

#[test]
fn make_temp_relation_appends_unique_suffix() {
    // dbt Core's dbt-bigquery adapter appends a `strftime("%H%M%S%f")` suffix to
    // `__dbt_tmp` temp relations (via `bigquery__make_relation_with_suffix`) to avoid
    // collisions across concurrent runs. Fusion previously fell back to the generic
    // `default__make_temp_relation`, which reused the bare `__dbt_tmp` identifier and
    // caused a conformance SQL mismatch against dbt Core (#8247).
    let harness = build_harness();
    let ctx = base_relation_ctx(&harness);

    let rendered = harness
        .render("{{ make_temp_relation(base_relation).identifier }}", ctx)
        .expect("render should succeed");
    let rendered = rendered.trim();

    let suffix = rendered
        .strip_prefix("member_snapshot__dbt_tmp")
        .unwrap_or_else(|| panic!("expected __dbt_tmp-prefixed identifier, got: {rendered:?}"));
    assert!(
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()),
        "BigQuery temp relation should append an all-digit timestamp suffix, got: {rendered:?}"
    );
}

#[test]
fn make_intermediate_relation_does_not_append_suffix() {
    // Unlike `make_temp_relation`, the intermediate/backup relation paths
    // (`dstring=False`) must keep the bare suffix untouched.
    let harness = build_harness();
    let ctx = base_relation_ctx(&harness);

    let rendered = harness
        .render(
            "{{ make_intermediate_relation(base_relation).identifier }}",
            ctx,
        )
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "member_snapshot__dbt_tmp");
}

// ---------------------------------------------------------------------------
// bigquery__generate_schema_name (LRC 4-part namespace)
// ---------------------------------------------------------------------------
const BQ_GENERATE_SCHEMA_NAME: &str = "{{ bigquery__generate_schema_name('staging', node) }}";
const DEFAULT__GENERATE_SCHEMA_NAME: &str = r#"
{% macro default__generate_schema_name(custom_schema_name, node) -%}
    custom_{{ custom_schema_name | trim }}
{%- endmacro %}
"#;

fn schema_name_harness(use_catalogs_v2: bool, override_default: bool) -> MacroTestHarness {
    let mut project_flags = BTreeMap::new();
    if use_catalogs_v2 {
        project_flags.insert("use_catalogs_v2".to_string(), Value::from(true));
    }

    let mut builder = MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_stub_functions()
        .with_global(
            "target",
            Value::from_serialize(BTreeMap::from([("schema", "jaffle_shop")])),
        )
        .with_global(
            "flags",
            Value::from_object(Flags::from_project_flags(project_flags)),
        );

    if override_default {
        builder = builder.with_macro(
            "test_project",
            "default__generate_schema_name",
            DEFAULT__GENERATE_SCHEMA_NAME,
        );
    }

    builder.build().expect("harness should build")
}

fn schema_name_ctx(catalog_name: Option<&'static str>) -> BTreeMap<String, Value> {
    let config = Arc::new(MockJinjaObject::new());
    config.on("get", move |args| {
        Ok(match args.first().and_then(|v| v.as_str()) {
            Some("catalog_name") => catalog_name.map(Value::from).unwrap_or(Value::UNDEFINED),
            _ => Value::UNDEFINED,
        })
    });

    let node = Arc::new(MockJinjaObject::new());
    node.set_attr("config", Value::from_dyn_object(config));

    BTreeMap::from([
        (
            "TARGET_PACKAGE_NAME".to_string(),
            Value::from("test_project"),
        ),
        ("node".to_string(), Value::from_dyn_object(node)),
    ])
}

fn with_lakehouse_catalog(harness: &MacroTestHarness, lakehouse_catalog: &'static str) {
    harness.mock().on("build_catalog_relation", move |_| {
        Ok(Value::from_serialize(BTreeMap::from([(
            "lakehouse_catalog",
            lakehouse_catalog,
        )])))
    });
}

#[test]
fn generate_schema_name_without_catalogs_v2_matches_default() {
    let harness = schema_name_harness(false, false);
    let bigquery = harness
        .render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(None))
        .expect("render should succeed");
    let default = harness
        .render(
            "{{ default__generate_schema_name('staging', node) }}",
            schema_name_ctx(None),
        )
        .expect("render should succeed");

    assert_eq!(bigquery.trim(), default.trim());
    assert_eq!(bigquery.trim(), "jaffle_shop_staging");
}

#[test]
fn generate_schema_name_with_catalogs_v2_prefixes_lakehouse_catalog() {
    let harness = schema_name_harness(true, false);
    with_lakehouse_catalog(&harness, "sales_catalog");

    let rendered = harness
        .render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(Some("BQ")))
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "sales_catalog.jaffle_shop_staging");
}

#[test]
fn generate_schema_name_with_catalogs_v2_but_no_lrc_matches_default() {
    let harness = schema_name_harness(true, false);

    let rendered = harness
        .render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(None))
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "jaffle_shop_staging");
}

#[test]
fn generate_schema_name_composes_on_a_projects_own_default() {
    let off = schema_name_harness(false, true);
    assert_eq!(
        off.render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(None))
            .expect("render should succeed")
            .trim(),
        "custom_staging"
    );

    let on = schema_name_harness(true, true);
    with_lakehouse_catalog(&on, "sales_catalog");
    assert_eq!(
        on.render(BQ_GENERATE_SCHEMA_NAME, schema_name_ctx(Some("BQ")))
            .expect("render should succeed")
            .trim(),
        "sales_catalog.custom_staging"
    );
}

#[test]
fn load_csv_rows_forwards_seed_path_from_adapter() {
    const SEED_PATH: &str = "/workspace/seeds/countries.csv";

    let harness = build_harness();
    harness
        .mock()
        .on("get_seed_file_path", |_| Ok(Value::from(SEED_PATH)))
        .on("load_dataframe", |_| Ok(Value::UNDEFINED));

    let model = Value::from_serialize(BTreeMap::from([
        ("database", Value::from("test-db")),
        ("schema", Value::from("test_schema")),
        ("alias", Value::from("countries")),
        (
            "config",
            Value::from_serialize(BTreeMap::<String, Value>::new()),
        ),
    ]));
    let ctx = BTreeMap::from([
        ("model".to_string(), model),
        ("agate_table".to_string(), Value::from("agate table")),
        ("dbt_version".to_string(), Value::from("2.0.0")),
        (
            "config".to_string(),
            Value::from_dyn_object(default_mock_config()),
        ),
    ]);

    harness
        .render("{{ bigquery__load_csv_rows(model, agate_table) }}", ctx)
        .expect("seed load macro should render");

    let calls = harness.mock().observed_calls();
    let load_call = calls
        .to("load_dataframe")
        .next()
        .expect("load_dataframe should be called");
    assert_eq!(
        load_call.args.get(3).and_then(Value::as_str),
        Some(SEED_PATH)
    );
}

fn render_governance_tags(
    relation_type: RelationType,
    column_dict: Value,
) -> dbt_common::FsResult<String> {
    let harness = build_harness();
    harness.mock().on("quote", |args| {
        Ok(Value::from(format!(
            "`{}`",
            args[0].as_str().expect("column name")
        )))
    });
    let relation = harness.relation("test-db", "test_schema", "customers", Some(relation_type));
    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        ("column_dict".to_string(), column_dict),
    ]);
    harness.render(
        "{{ bigquery__get_alter_column_data_governance_tags_sql(relation, column_dict) }}",
        ctx,
    )
}

fn column_dict(columns: BTreeMap<&str, Value>) -> Value {
    Value::from_serialize(columns)
}

fn tag_map(entries: &[(&str, &str)]) -> Value {
    let map: BTreeMap<&str, &str> = entries.iter().copied().collect();
    Value::from_serialize(map)
}

#[test]
fn data_governance_tags_empty_when_no_column_declares_tags() {
    let plain = Value::from_serialize(BTreeMap::from([("description", "plain column")]));
    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("id", plain)])),
    )
    .expect("columns without data_governance_tags should render empty SQL");
    assert!(rendered.trim().is_empty(), "got unexpected SQL: {rendered}");
}

#[test]
fn data_governance_tags_alter_batches_multiple_columns_and_skips_untagged() {
    let email = Value::from_serialize(BTreeMap::from([
        ("data_type", Value::from("STRING")),
        (
            "data_governance_tags",
            tag_map(&[("my-project/data_class", "email")]),
        ),
    ]));
    let ssn = Value::from_serialize(BTreeMap::from([
        ("data_type", Value::from("STRING")),
        (
            "data_governance_tags",
            tag_map(&[("94408448616/data_class", "ssn")]),
        ),
    ]));
    let plain = Value::from_serialize(BTreeMap::from([(
        "description",
        Value::from("untagged column"),
    )]));

    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([
            ("email", email),
            ("name", plain),
            ("ssn", ssn),
        ])),
    )
    .expect("governance tag SQL should render");

    let sql = rendered.trim();
    assert!(
        sql.starts_with("alter table "),
        "expected a single ALTER TABLE statement, got: {sql}"
    );
    assert!(
        sql.contains(
            "alter column `email` set options (data_governance_tags=[(\"my-project/data_class\", \"email\")])"
        ),
        "missing email clause, got: {sql}"
    );
    assert!(
        sql.contains(
            "alter column `ssn` set options (data_governance_tags=[(\"94408448616/data_class\", \"ssn\")])"
        ),
        "missing ssn clause, got: {sql}"
    );
    assert!(
        !sql.contains("`name`"),
        "untagged column must not appear in ALTER TABLE, got: {sql}"
    );
}

#[test]
fn data_governance_tags_empty_map_clears_column_tags() {
    let cleared = Value::from_serialize(BTreeMap::from([("data_governance_tags", tag_map(&[]))]));
    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", cleared)])),
    )
    .expect("empty mapping should emit clear clause");
    assert!(
        rendered
            .trim()
            .contains("alter column `email` set options (data_governance_tags=[])"),
        "expected empty tuple array, got: {rendered}"
    );
}

#[test]
fn data_governance_tags_escapes_string_literals() {
    let email = Value::from_serialize(BTreeMap::from([(
        "data_governance_tags",
        tag_map(&[("my-project/data_class", "say \"hi\"\\\nline2\r")]),
    )]));
    // Use an interior newline/CR so leading/trailing whitespace validation passes.
    let email_interior = Value::from_serialize(BTreeMap::from([(
        "data_governance_tags",
        tag_map(&[("my-project/data_class", "say \"hi\"\\\nline2\rline3")]),
    )]));
    let err_trailing = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", email)])),
    )
    .expect_err("trailing \\r must fail leading/trailing whitespace validation");
    assert!(err_trailing.to_string().contains("leading or trailing"));

    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", email_interior)])),
    )
    .expect("escaped tag value should render");
    assert!(
        rendered.contains(r#"("my-project/data_class", "say \"hi\"\\\nline2\rline3")"#),
        "unexpected escaping in SQL: {rendered}"
    );
}

#[test]
fn data_governance_tags_rejects_views_and_materialized_views() {
    let email = Value::from_serialize(BTreeMap::from([(
        "data_governance_tags",
        tag_map(&[("my-project/data_class", "email")]),
    )]));
    for rel_type in [
        RelationType::View,
        RelationType::MaterializedView,
        RelationType::External,
    ] {
        let err = render_governance_tags(
            rel_type,
            column_dict(BTreeMap::from([("email", email.clone())])),
        )
        .expect_err("non-table relation with data_governance_tags must fail");
        assert!(
            err.to_string().contains("only supports on table columns"),
            "unexpected error: {err}"
        );
    }

    // Views with description only remain unaffected.
    let desc_only =
        Value::from_serialize(BTreeMap::from([("description", Value::from("view email"))]));
    let rendered = render_governance_tags(
        RelationType::View,
        column_dict(BTreeMap::from([("email", desc_only)])),
    )
    .expect("view with description only should render empty");
    assert!(rendered.trim().is_empty());
}

#[test]
fn data_governance_tags_rejects_struct_parent_and_nested_dotted_path() {
    for dtype in ["STRUCT<email STRING>", "ARRAY<STRUCT <email STRING>>"] {
        let struct_col = Value::from_serialize(BTreeMap::from([
            ("data_type", Value::from(dtype)),
            (
                "data_governance_tags",
                tag_map(&[("my-project/data_class", "pii")]),
            ),
        ]));
        let err_struct = render_governance_tags(
            RelationType::Table,
            column_dict(BTreeMap::from([("profile", struct_col)])),
        )
        .expect_err("STRUCT parent column must fail");
        assert!(
            err_struct.to_string().contains("RECORD/STRUCT"),
            "unexpected error for dtype {dtype}: {err_struct}"
        );
    }

    let nested_col = Value::from_serialize(BTreeMap::from([
        ("data_type", Value::from("STRING")),
        (
            "data_governance_tags",
            tag_map(&[("my-project/data_class", "email")]),
        ),
    ]));
    let err_nested = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("profile.email", nested_col)])),
    )
    .expect_err("nested dotted field path must fail for ALTER COLUMN DDL");
    assert!(
        err_nested.to_string().contains("nested STRUCT field"),
        "unexpected error: {err_nested}"
    );
}

#[test]
fn data_governance_tags_rejects_mutual_exclusivity_with_policy_tags() {
    let col = Value::from_serialize(BTreeMap::from([
        (
            "policy_tags",
            Value::from_serialize(vec!["projects/p/locations/us/taxonomies/1/policyTags/2"]),
        ),
        (
            "data_governance_tags",
            tag_map(&[("my-project/data_class", "email")]),
        ),
    ]));
    let err = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", col)])),
    )
    .expect_err("policy_tags combined with data_governance_tags must fail");
    assert!(
        err.to_string()
            .contains("cannot specify both `policy_tags` and `data_governance_tags`"),
        "unexpected error: {err}"
    );

    // Non-BigQuery mapping-valued policy_tags must NOT trigger false positives.
    let map_pt_col = Value::from_serialize(BTreeMap::from([
        (
            "policy_tags",
            Value::from_serialize(vec![BTreeMap::from([("masking_policy", "mask_email")])]),
        ),
        (
            "data_governance_tags",
            tag_map(&[("my-project/data_class", "email")]),
        ),
    ]));
    let ok = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", map_pt_col)])),
    )
    .expect("mapping-valued policy_tags must be ignored");
    assert!(ok.contains(r#"("my-project/data_class", "email")"#));
}

#[test]
fn data_governance_tags_forwards_multi_entry_mapping_without_truncation() {
    let col = Value::from_serialize(BTreeMap::from([(
        "data_governance_tags",
        tag_map(&[
            ("my-project/data_class", "email"),
            ("my-project/domain", "marketing"),
        ]),
    )]));
    let rendered = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", col)])),
    )
    .expect("multi-entry tag map must forward all entries");
    assert!(rendered.contains(r#"("my-project/data_class", "email")"#));
    assert!(rendered.contains(r#"("my-project/domain", "marketing")"#));
}

#[test]
fn data_governance_tags_rejects_invalid_keys_values_and_types() {
    let invalid_cases = [
        ("my-project", "email", "expected '<parent_id>/<tag_key>'"),
        ("a/b/c", "email", "expected '<parent_id>/<tag_key>'"),
        ("/key", "email", "expected '<parent_id>/<tag_key>'"),
        ("proj/", "email", "expected '<parent_id>/<tag_key>'"),
        ("my proj/key", "email", "with no whitespace"),
        ("my\u{00a0}proj/key", "email", "with no whitespace"),
        ("my-project/key", "", "non-empty tag value short name"),
        ("my-project/key", "   ", "non-empty tag value short name"),
        (
            "my-project/key",
            " email ",
            "leading or trailing whitespace",
        ),
        ("my-project/key", "pii/email", "leaf tag value short name"),
    ];
    for (key, value, expected) in invalid_cases {
        let col = Value::from_serialize(BTreeMap::from([(
            "data_governance_tags",
            tag_map(&[(key, value)]),
        )]));
        let err = render_governance_tags(
            RelationType::Table,
            column_dict(BTreeMap::from([("email", col)])),
        )
        .expect_err("invalid tag entry must fail");
        assert!(
            err.to_string().contains(expected),
            "case ({key:?}, {value:?}) gave unexpected error: {err}"
        );
    }

    let bad_type = Value::from_serialize(BTreeMap::from([(
        "data_governance_tags",
        Value::from_serialize(vec!["my-project/data_class"]),
    )]));
    let err_type = render_governance_tags(
        RelationType::Table,
        column_dict(BTreeMap::from([("email", bad_type)])),
    )
    .expect_err("non-mapping data_governance_tags must fail");
    assert!(
        err_type.to_string().contains("expected a mapping"),
        "unexpected error: {err_type}"
    );
}

fn mock_bq_column(flat_names: &[&'static str]) -> Value {
    let col = Arc::new(MockJinjaObject::new());
    let flat_vals: Vec<Value> = flat_names
        .iter()
        .map(|name| Value::from_serialize(BTreeMap::from([("name", *name)])))
        .collect();
    col.on("flatten", move |_| Ok(Value::from(flat_vals.clone())));
    Value::from_dyn_object(col)
}

#[test]
fn data_governance_tags_persist_docs_rejects_struct_parent_without_yaml_data_type() {
    let harness = build_harness();
    let config = Arc::new(MockJinjaObject::new());
    config.on("persist_relation_docs", |_| Ok(Value::from(false)));
    config.on("persist_column_docs", |_| Ok(Value::from(true)));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from(vec![
            mock_bq_column(&["email"]),
            mock_bq_column(&["profile.work_email", "profile.identity.ssn"]),
        ]))
    });
    harness.mock().on("quote", |args| {
        Ok(Value::from(format!(
            "`{}`",
            args[0].as_str().expect("column name")
        )))
    });
    harness
        .mock()
        .on("update_columns", |_| Ok(Value::UNDEFINED));

    let model = Value::from_serialize(BTreeMap::from([(
        "columns",
        column_dict(BTreeMap::from([
            // No `data_type` declared in schema.yml — must still be rejected as RECORD/STRUCT!
            (
                "profile",
                Value::from_serialize(BTreeMap::from([(
                    "data_governance_tags",
                    tag_map(&[("my-project/data_class", "pii")]),
                )])),
            ),
            (
                "email",
                Value::from_serialize(BTreeMap::from([("description", Value::from("Email"))])),
            ),
        ])),
    )]));
    let relation = harness.relation(
        "test-db",
        "test_schema",
        "customers",
        Some(RelationType::Table),
    );
    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        ("model".to_string(), model),
        ("config".to_string(), Value::from_dyn_object(config)),
        ("dbt_version".to_string(), Value::from("2.0.0")),
    ]);

    let err = harness
        .render(
            "{{ bigquery__persist_docs(relation, model, false, true) }}",
            ctx,
        )
        .expect_err("uncontracted STRUCT parent with DGT must fail before mutation");
    assert!(
        err.to_string().contains("RECORD/STRUCT"),
        "unexpected error: {err}"
    );
    harness
        .mock()
        .observed_calls()
        .assert_not_called("update_columns");
    harness.mock().observed_calls().assert_not_called("execute");
}

#[test]
fn data_governance_tags_persist_docs_clears_before_policy_tags_on_rollback() {
    use std::sync::Mutex;

    let order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let harness = build_harness();
    let config = Arc::new(MockJinjaObject::new());
    config.on("persist_relation_docs", |_| Ok(Value::from(false)));
    config.on("persist_column_docs", |_| Ok(Value::from(true)));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from(vec![
            mock_bq_column(&["ssn"]),
            mock_bq_column(&["email"]),
        ]))
    });
    harness.mock().on("quote", |args| {
        Ok(Value::from(format!(
            "`{}`",
            args[0].as_str().expect("column name")
        )))
    });
    let order_uc = order.clone();
    harness.mock().on("update_columns", move |_| {
        order_uc.lock().unwrap().push("update_columns".to_string());
        Ok(Value::UNDEFINED)
    });
    let order_exec = order.clone();
    harness.mock().on("execute", move |args| {
        let sql = args
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        order_exec.lock().unwrap().push(format!("execute:{sql}"));
        Ok(Value::from_serialize(("SUCCESS", Vec::<Value>::new())))
    });

    let model = Value::from_serialize(BTreeMap::from([(
        "columns",
        column_dict(BTreeMap::from([
            // Rollback column: clear DGT before attaching legacy policy_tags.
            (
                "ssn",
                Value::from_serialize(BTreeMap::from([
                    (
                        "policy_tags",
                        Value::from_serialize(vec![
                            "projects/p/locations/us/taxonomies/1/policyTags/2",
                        ]),
                    ),
                    ("data_governance_tags", tag_map(&[])),
                ])),
            ),
            // Sibling column updated via post-update DDL.
            (
                "email",
                Value::from_serialize(BTreeMap::from([(
                    "data_governance_tags",
                    tag_map(&[("my-project/data_class", "email")]),
                )])),
            ),
        ])),
    )]));
    let relation = harness.relation(
        "test-db",
        "test_schema",
        "customers",
        Some(RelationType::Table),
    );
    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        ("model".to_string(), model),
        ("config".to_string(), Value::from_dyn_object(config)),
        ("dbt_version".to_string(), Value::from("2.0.0")),
        ("execute".to_string(), Value::from(true)),
    ]);

    harness
        .render(
            "{{ bigquery__persist_docs(relation, model, false, true) }}",
            ctx,
        )
        .expect("rollback persist_docs should succeed");

    let recorded = order.lock().unwrap();
    assert_eq!(
        recorded.len(),
        3,
        "expected clear -> update_columns -> set, got: {recorded:?}"
    );
    assert!(
        recorded[0].contains("alter column `ssn` set options (data_governance_tags=[])"),
        "expected pre-clear first, got: {:?}",
        recorded[0]
    );
    assert_eq!(recorded[1], "update_columns");
    assert!(
        recorded[2].contains(
            r#"alter column `email` set options (data_governance_tags=[("my-project/data_class", "email")])"#
        ),
        "expected post-set last, got: {:?}",
        recorded[2]
    );
}

#[test]
fn data_governance_tags_persist_docs_skips_ddl_under_dbt_1x() {
    let harness = build_harness();
    let config = Arc::new(MockJinjaObject::new());
    config.on("persist_relation_docs", |_| Ok(Value::from(false)));
    config.on("persist_column_docs", |_| Ok(Value::from(true)));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from(vec![mock_bq_column(&["email"])]))
    });
    harness
        .mock()
        .on("update_columns", |_| Ok(Value::UNDEFINED));

    let model = Value::from_serialize(BTreeMap::from([(
        "columns",
        column_dict(BTreeMap::from([(
            "email",
            Value::from_serialize(BTreeMap::from([(
                "data_governance_tags",
                tag_map(&[("my-project/data_class", "email")]),
            )])),
        )])),
    )]));
    let relation = harness.relation(
        "test-db",
        "test_schema",
        "customers",
        Some(RelationType::Table),
    );
    let ctx = BTreeMap::from([
        (
            "relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        ("model".to_string(), model),
        ("config".to_string(), Value::from_dyn_object(config)),
        ("dbt_version".to_string(), Value::from("1.13.0")),
        ("execute".to_string(), Value::from(true)),
    ]);

    harness
        .render(
            "{{ bigquery__persist_docs(relation, model, false, true) }}",
            ctx,
        )
        .expect("dbt 1.x path should only invoke adapter.update_columns");

    harness
        .mock()
        .observed_calls()
        .assert_called("update_columns");
    harness.mock().observed_calls().assert_not_called("execute");
}
