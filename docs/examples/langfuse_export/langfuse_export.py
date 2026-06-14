"""
---
title: Export Langfuse Observability Data
description: Export traces, evaluations, datasets, and cost data from Langfuse to your data lake
keywords: [langfuse, llm, observability, traces, evaluations, datasets, scores, export]
---

[Langfuse](https://langfuse.com/) is an open-source LLM observability and evaluation platform.
It captures traces and observations from your AI applications, manages evaluation datasets, and
tracks LLM costs — all in a self-hostable Postgres-backed store.

To enable analytics, reporting, and offline evaluation, this data needs to be exported to a
data lakehouse or warehouse. Langfuse persists relational metadata in **PostgreSQL**.

The `dlt` library's built-in `sql_database()` source makes extraction straightforward:

* Connect directly to Langfuse's Postgres backend with a connection string.
* Use `resolve_foreign_keys=True` so dlt automatically links child tables (e.g. datasets → projects).
* Load to **DuckDB** for zero-config local analytics, or swap to any other dlt destination.

## Credentials

Add the following block to `.dlt/secrets.toml`, filling in the values from your deployment:

```toml
# .dlt/secrets.toml
[sources.sql_database.credentials]
drivername = "postgresql"
host = "localhost"
port = 5432
database = "langfuse"
username = "langfuse"
password = "langfuse"
```

You also need the psycopg2 driver installed:

```sh
pip install psycopg2-binary
```

Where to find these values:
- **Docker Compose**: look for the `db` service in your `docker-compose.yml`. The credentials are
  set via `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` environment variables. The host
  is the service name (or `localhost` if the port is published to the host).
- **Helm chart**: check `values.yaml` under `postgresql.auth` or the `DATABASE_URL`
  environment variable on the Langfuse deployment.
- **Langfuse Cloud**: direct database access is not available on Langfuse Cloud; use the
  [Langfuse API](https://api.reference.langfuse.com/) instead.

## Data model

dlt automatically discovers every table in the Langfuse database and infers their relationships from
foreign keys — no schema configuration required. The exact set of tables you get depends on which
Langfuse features you have used: tables are only populated once the corresponding feature is
exercised.

The tables most relevant for LLM observability and evaluation are:

- **`organizations`** — top-level multi-tenant containers; each org owns projects and members.
- **`projects`** — a project scopes all traces, scores, and datasets for one application.
- **`trace_sessions`** — groups related traces into a session (e.g. a multi-turn conversation),
  tagged with an `environment` field for staging vs. production separation.
- **`datasets`** — curated collections of examples used for evaluation runs, created via the
  Langfuse web UI or SDK.
- **`dataset_items`** — individual rows inside a dataset, each carrying an `input`,
  optional `expected_output`, and a link back to the source trace/observation that produced it.
- **`score_configs`** — named score type definitions (numeric or categorical) with optional
  `min_value`/`max_value` bounds; child table `score_configs__categories` holds the category labels.
- **`eval_templates`** — versioned LLM-as-judge templates with a `prompt`, `model`, `provider`,
  and typed output schema; child table `eval_templates__vars` holds the variable names.
- **`models`** — LLM model definitions with `input_price`, `output_price`, and tokenizer config
  used to compute per-trace cost.
- **`prices`** / **`pricing_tiers`** — tiered pricing rules linked to model definitions.
- **`annotation_queues`** — human annotation workflows; items are tracked in
  `annotation_queue_items` and assignees in `annotation_queue_assignments`.
- **`comments`** — user comments attached to any Langfuse object (trace, dataset, etc.).
- **`dashboards`** / **`dashboard_widgets`** — saved analytics dashboards with chart configuration.
- **`audit_logs`** — full audit trail of create/update/delete actions across the platform.

Some tables are internal to Langfuse (e.g. `_prisma_migrations`, `background_migrations`).
You can explicitly select a subset of tables to load via `sql_database(..., table_names=...)`.

```mermaid
erDiagram
    _dlt_version{
    bigint version
    bigint engine_version
    timestamp inserted_at
    text schema_name
    text version_hash
    text schema
}
    _dlt_loads{
    text load_id
    text schema_name
    bigint status
    timestamp inserted_at
    text schema_version_hash
}
    _prisma_migrations{
    text id PK
    text checksum
    timestamp finished_at
    text migration_name
    timestamp started_at
    bigint applied_steps_count
    text _dlt_load_id
    text _dlt_id UK
}
    users{
    text id PK
    text name
    text email
    text password
    timestamp created_at
    timestamp updated_at
    bool admin
    bool v4_beta_enabled
    text _dlt_load_id
    text _dlt_id UK
}
    projects{
    text id PK
    timestamp created_at
    text name
    timestamp updated_at
    text org_id
    bool has_traces
    text _dlt_load_id
    text _dlt_id UK
}
    organizations{
    text id PK
    text name
    timestamp created_at
    timestamp updated_at
    bool ai_features_enabled
    timestamp cloud_billing_cycle_anchor
    text _dlt_load_id
    text _dlt_id UK
}
    score_configs{
    text id PK
    timestamp created_at
    timestamp updated_at
    text project_id
    text name
    text data_type
    bool is_archived
    text description
    text _dlt_load_id
    text _dlt_id UK
}
    api_keys{
    text id PK
    timestamp created_at
    text public_key
    text hashed_secret_key
    text display_secret_key
    text project_id
    text fast_hashed_secret_key
    text scope
    text _dlt_load_id
    text _dlt_id UK
}
    organization_memberships{
    text id PK
    text org_id
    text user_id
    text role
    timestamp created_at
    timestamp updated_at
    text _dlt_load_id
    text _dlt_id UK
}
    datasets{
    text id PK
    text name
    text project_id PK
    timestamp created_at
    timestamp updated_at
    text description
    bool remote_experiment_enabled
    text metadata__foo
    text _dlt_load_id
    text _dlt_id UK
}
    dataset_items{
    text id PK
    text source_observation_id
    text dataset_id
    timestamp created_at
    timestamp updated_at
    text status
    text source_trace_id
    text project_id PK
    bool is_deleted
    timestamp valid_from PK
    text metadata__resource_attributes__service_name
    text metadata__resource_attributes__telemetry_sdk_name
    text metadata__resource_attributes__telemetry_sdk_version
    text metadata__resource_attributes__telemetry_sdk_language
    text metadata__attributes__gen_ai_system
    text metadata__attributes__operation_cost
    text metadata__attributes__server_address
    text metadata__attributes__gen_ai_agent_name
    text metadata__attributes__gen_ai_response_id
    text metadata__attributes__logfire_json_schema
    text metadata__attributes__gen_ai_agent_call_id
    text metadata__attributes__gen_ai_provider_name
    text metadata__attributes__gen_ai_request_model
    text metadata__attributes__gen_ai_operation_name
    text metadata__attributes__gen_ai_response_model
    text metadata__attributes__gen_ai_conversation_id
    text metadata__attributes__gen_ai_tool_definitions
    text metadata__attributes__model_request_parameters
    text metadata__attributes__gen_ai_usage_input_tokens
    text metadata__attributes__gen_ai_usage_output_tokens
    text metadata__attributes__gen_ai_response_finish_reasons
    text metadata__scope__name
    text metadata__scope__version
    text _dlt_load_id
    text _dlt_id UK
    text expected_output
    text metadata__attributes__logfire_msg
    text metadata__attributes__tool_response
    text metadata__attributes__tool_arguments
    text metadata__attributes__gen_ai_tool_name
    text metadata__attributes__gen_ai_tool_call_id
    bigint input__square
}
    trace_sessions{
    text id PK
    timestamp created_at
    timestamp updated_at
    text project_id PK
    bool bookmarked
    bool public
    text environment
    text _dlt_load_id
    text _dlt_id UK
}
    models{
    text id PK
    timestamp created_at
    timestamp updated_at
    text model_name
    text match_pattern
    timestamp start_date
    decimal input_price
    decimal output_price
    text unit
    text tokenizer_id
    bigint tokenizer_config__tokens_per_name
    text tokenizer_config__tokenizer_model
    bigint tokenizer_config__tokens_per_message
    text _dlt_load_id
    text _dlt_id UK
    decimal total_price
}
    audit_logs{
    text id PK
    timestamp created_at
    timestamp updated_at
    text user_id
    text resource_type
    text resource_id
    text action
    text after
    text org_id
    text user_org_role
    text type
    text _dlt_load_id
    text _dlt_id UK
    text project_id
    text user_project_role
    text before
}
    eval_templates{
    text id PK
    timestamp created_at
    timestamp updated_at
    text name
    bigint version
    text prompt
    text output_schema__score
    text output_schema__reasoning
    text _dlt_load_id
    text _dlt_id UK
    text partner
}
    comments{
    text id PK
    text project_id
    text object_type
    text object_id
    timestamp created_at
    timestamp updated_at
    text content
    text author_user_id
    text _dlt_load_id
    text _dlt_id UK
}
    annotation_queues{
    text id PK
    text name
    text description
    text project_id
    timestamp created_at
    timestamp updated_at
    text _dlt_load_id
    text _dlt_id UK
}
    annotation_queue_items{
    text id PK
    text queue_id
    text object_id
    text object_type
    text status
    text project_id
    timestamp created_at
    timestamp updated_at
    text _dlt_load_id
    text _dlt_id UK
}
    background_migrations{
    text id PK
    text name
    text script
    timestamp finished_at
    text worker_id
    text _dlt_load_id
    text _dlt_id UK
    timestamp state__max_date
    bigint state__offset
}
    prices{
    text id PK
    timestamp created_at
    timestamp updated_at
    text model_id
    text usage_type
    decimal price
    text pricing_tier_id
    text _dlt_load_id
    text _dlt_id UK
}
    pricing_tiers{
    text id PK
    timestamp created_at
    timestamp updated_at
    text model_id
    text name
    bool is_default
    bigint priority
    text _dlt_load_id
    text _dlt_id UK
}
    dashboard_widgets{
    text id PK
    timestamp created_at
    timestamp updated_at
    text name
    text description
    text view
    text chart_type
    bigint min_version
    text chart_config__type
    text _dlt_load_id
    text _dlt_id UK
    bigint chart_config__row_limit
}
    dashboards{
    text id PK
    timestamp created_at
    timestamp updated_at
    text name
    text description
    text _dlt_load_id
    text _dlt_id UK
}
    annotation_queue_assignments{
    text id PK
    text project_id
    text user_id
    text queue_id
    timestamp created_at
    timestamp updated_at
    text _dlt_load_id
    text _dlt_id UK
}
    notification_preferences{
    text id PK
    text user_id
    text project_id
    text channel
    text type
    bool enabled
    timestamp created_at
    timestamp updated_at
    text _dlt_load_id
    text _dlt_id UK
}
    _dlt_pipeline_state{
    bigint version
    bigint engine_version
    text pipeline_name
    text state
    timestamp created_at
    text version_hash
    text _dlt_load_id
    text _dlt_id UK
}
    eval_templates__vars{
    text value
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    score_configs__categories{
    text label
    bigint value
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dashboard_widgets__dimensions{
    text field
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dashboard_widgets__metrics{
    text agg
    text measure
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    annotation_queues__score_config_ids{
    text value
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    pricing_tiers__conditions{
    bigint value
    text operator
    bool case_sensitive
    text usage_detail_pattern
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dataset_items__expected_output{
    text role
    text finish_reason
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dataset_items__expected_output__parts{
    text id
    text name
    text type
    text arguments
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dataset_items__input__tools{
    text name
    text type
    text description
    text parameters__type
    bool parameters__additional_properties
    text parameters__properties__square__type
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dataset_items__input__tools__parameters__required{
    text value
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dataset_items__input__messages{
    text role
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dataset_items__input__messages__parts{
    text type
    text content
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    dashboards__definition__widgets{
    bigint x
    bigint y
    text id
    text type
    bigint x_size
    bigint y_size
    text widget_id
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    _prisma_migrations }|--|| _dlt_loads : "_dlt_load"
    users }|--|| _dlt_loads : "_dlt_load"
    projects }|--|| _dlt_loads : "_dlt_load"
    projects ||--|{ organizations : ""
    organizations }|--|| _dlt_loads : "_dlt_load"
    score_configs }|--|| _dlt_loads : "_dlt_load"
    score_configs ||--|{ projects : ""
    api_keys }|--|| _dlt_loads : "_dlt_load"
    api_keys ||--|{ organizations : ""
    api_keys ||--|{ projects : ""
    organization_memberships }|--|| _dlt_loads : "_dlt_load"
    organization_memberships ||--|{ users : ""
    organization_memberships ||--|{ organizations : ""
    datasets }|--|| _dlt_loads : "_dlt_load"
    datasets ||--|{ projects : ""
    dataset_items }|--|| _dlt_loads : "_dlt_load"
    dataset_items ||--|{ datasets : ""
    trace_sessions }|--|| _dlt_loads : "_dlt_load"
    trace_sessions ||--|{ projects : ""
    models }|--|| _dlt_loads : "_dlt_load"
    models ||--|{ projects : ""
    audit_logs }|--|| _dlt_loads : "_dlt_load"
    eval_templates }|--|| _dlt_loads : "_dlt_load"
    eval_templates ||--|{ projects : ""
    comments }|--|| _dlt_loads : "_dlt_load"
    comments ||--|{ projects : ""
    annotation_queues }|--|| _dlt_loads : "_dlt_load"
    annotation_queues ||--|{ projects : ""
    annotation_queue_items }|--|| _dlt_loads : "_dlt_load"
    annotation_queue_items ||--|{ annotation_queues : ""
    annotation_queue_items ||--|{ projects : ""
    annotation_queue_items ||--|{ users : ""
    background_migrations }|--|| _dlt_loads : "_dlt_load"
    prices }|--|| _dlt_loads : "_dlt_load"
    prices ||--|{ pricing_tiers : ""
    prices ||--|{ models : ""
    prices ||--|{ projects : ""
    pricing_tiers }|--|| _dlt_loads : "_dlt_load"
    pricing_tiers ||--|{ models : ""
    dashboard_widgets }|--|| _dlt_loads : "_dlt_load"
    dashboard_widgets ||--|{ users : ""
    dashboard_widgets ||--|{ projects : ""
    dashboards }|--|| _dlt_loads : "_dlt_load"
    dashboards ||--|{ users : ""
    dashboards ||--|{ projects : ""
    annotation_queue_assignments }|--|| _dlt_loads : "_dlt_load"
    annotation_queue_assignments ||--|{ annotation_queues : ""
    annotation_queue_assignments ||--|{ projects : ""
    annotation_queue_assignments ||--|{ users : ""
    notification_preferences }|--|| _dlt_loads : "_dlt_load"
    notification_preferences ||--|{ users : ""
    notification_preferences ||--|{ projects : ""
    _dlt_pipeline_state }|--|| _dlt_loads : "_dlt_load"
    eval_templates__vars }|--|| eval_templates : "_dlt_parent"
    score_configs__categories }|--|| score_configs : "_dlt_parent"
    dashboard_widgets__dimensions }|--|| dashboard_widgets : "_dlt_parent"
    dashboard_widgets__metrics }|--|| dashboard_widgets : "_dlt_parent"
    annotation_queues__score_config_ids }|--|| annotation_queues : "_dlt_parent"
    pricing_tiers__conditions }|--|| pricing_tiers : "_dlt_parent"
    dataset_items__expected_output }|--|| dataset_items : "_dlt_parent"
    dataset_items__expected_output__parts }|--|| dataset_items__expected_output : "_dlt_parent"
    dataset_items__input__tools }|--|| dataset_items : "_dlt_parent"
    dataset_items__input__tools__parameters__required }|--|| dataset_items__input__tools : "_dlt_parent"
    dataset_items__input__messages }|--|| dataset_items : "_dlt_parent"
    dataset_items__input__messages__parts }|--|| dataset_items__input__messages : "_dlt_parent"
    dashboards__definition__widgets }|--|| dashboards : "_dlt_parent"
    _dlt_version ||--|{ _dlt_loads : "_dlt_schema_version"
    _dlt_version }|--|{ _dlt_loads : "_dlt_schema_name"
```

"""
import dlt
from dlt.sources.sql_database import sql_database


@dlt.source
def langfuse_source(credentials=dlt.secrets.value):
    return sql_database(
        credentials=credentials,
        reflection_level="minimal",
        resolve_foreign_keys=True,
    )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="langfuse",
        # can be configure to any dlt destination
        destination="duckdb",
    )
    load_info = pipeline.run(langfuse_source)
    print(load_info)
