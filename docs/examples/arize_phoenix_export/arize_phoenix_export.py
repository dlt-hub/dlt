"""
---
title: Export Arize Phoenix Telemetry
description: Export traces, spans, and evaluations from Arize Phoenix to your data lake
keywords: [arize, phoenix, opentelemetry, traces, spans, evaluations, observability, llm, export]
---

[Arize Phoenix](https://phoenix.arize.com/) is an open-source observability platform for AI and LLM
applications. It captures traces, spans, and evaluations (e.g. relevance scores, hallucination checks)
that are sent via the OpenTelemetry (OTel) protocol.

To enable analytics, reporting, and offline evaluation, this telemetry needs to be exported to a
data lakehouse or warehouse. Arize Phoenix persists data in a relational database — either **PostgreSQL**
(recommended for production) or **SQLite** (the default for single-node deployments).

The `dlt` library's built-in `sql_database()` source makes extraction straightforward:

* Connect directly to Phoenix's Postgres or SQLite backend with a connection string.
* Use `resolve_foreign_keys=True` so dlt automatically links child tables (e.g. spans → traces).
* Load to **DuckDB** for zero-config local analytics, or swap to any other dlt destination.

## Credentials

### PostgreSQL

Add the following block to `.dlt/secrets.toml`, filling in the values from your deployment:

```toml
# .dlt/secrets.toml
[sources.sql_database.credentials]
drivername = "postgresql"
host = "localhost"
port = 5432
database = "phoenix"
username = "phoenix"
password = "phoenix"
```

You also need the psycopg2 driver installed:

```bash
pip install psycopg2-binary
```

Where to find these values:
- **Docker Compose**: look for the `db` service in your `docker-compose.yml`. The credentials are
  set via `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` environment variables. The host
  is the service name (or `localhost` if the port is published to the host).
- **Helm chart**: check `values.yaml` under `postgresql.auth` or the `PHOENIX_SQL_DATABASE_URL`
  environment variable on the Phoenix deployment.

### SQLite

Add the following block to `.dlt/secrets.toml`, pointing `database` at the `.db` file on disk:

```toml
# .dlt/secrets.toml
[sources.sql_database.credentials]
drivername = "sqlite"
database = "/path/to/phoenix.db"
```

Where to find the file:
- **Docker Compose**: the SQLite file lives at `/phoenix/storage/phoenix.db` inside the container.
  Volume-mount it to a host path so the pipeline can read it directly:
  ```yaml
  volumes:
    - ./data/phoenix.db:/phoenix/storage/phoenix.db
  ```
  Then set `database` to the host-side path (e.g. `./data/phoenix.db`).
- **No volume mount**: copy the file out first with
  `docker cp <container>:/phoenix/storage/phoenix.db ./phoenix.db`, then point `database` at the
  copied file.

NOTE. You need the `dlt[sql-database]` extra installed:

```bash
pip install "dlt[sql-database]"
```

## Data model

dlt automatically discovers every table in the Phoenix database and infers their relationships from
foreign keys — no schema configuration required. The exact set of tables you get depends on which
Phoenix features you have used: tables are only created in the database once the corresponding
feature is exercised. For example, the `datasets` and `dataset_examples` tables do not exist until
you create a dataset through the Phoenix web UI.

The tables most relevant for AI observability are:

- **`projects`** — top-level containers; each project groups a set of traces.
- **`traces`** — a trace is a complete request or workflow, identified by a `trace_id`.
- **`spans`** — the core table. Every OTEL span belongs to a trace. Spans carry timing, status,
  LLM token counts, input/output values, and arbitrary OpenInference attributes.
- **`span_annotations`** — evaluations attached to individual spans (e.g. relevance score,
  hallucination label), produced by human raters or automated LLM judges.
- **`trace_annotations`** — same as `span_annotations` but scoped to a full trace.
- **`datasets`** — curated collections of examples, created via the Phoenix web UI or SDK.
- **`dataset_examples`** — the individual rows inside a dataset, each linked back to a source span.
- **`dataset_versions`** — snapshot history of a dataset, enabling reproducible evaluations.
- **`experiments`** — prompt playground runs executed against a dataset version.
- **`experiment_runs`** — individual outputs produced by an experiment for each dataset example.

Some tables are internal to Phoenix (e.g. migration tracking, internal job queues).
You can explicitly select a subset of tables to load via the parameter `sql_database(..., table_names=...)`.

```mermaid
erDiagram
    alembic_version{
    text version_num PK
    text _dlt_load_id
    text _dlt_id UK
}
    users{
    bigint id PK
    bigint user_role_id
    text username
    text email
    text profile_picture_url
    binary password_hash
    binary password_salt
    bool reset_password
    text oauth2_client_id
    text oauth2_user_id
    timestamp created_at
    timestamp updated_at
    text auth_method
    text ldap_unique_id
    text _dlt_load_id
    text _dlt_id UK
}
    user_roles{
    bigint id PK
    text name
    text _dlt_load_id
    text _dlt_id UK
}
    projects{
    bigint id PK
    text name
    text description
    text gradient_start_color
    text gradient_end_color
    timestamp created_at
    timestamp updated_at
    bigint trace_retention_policy_id
    text _dlt_load_id
    text _dlt_id UK
}
    project_trace_retention_policies{
    bigint id PK
    text name
    text cron_expression
    json rule
    text _dlt_load_id
    text _dlt_id UK
}
    generative_models{
    bigint id PK
    text name
    text provider
    text name_pattern
    bool is_built_in
    timestamp start_time
    timestamp created_at
    timestamp updated_at
    timestamp deleted_at
    text _dlt_load_id
    text _dlt_id UK
}
    token_prices{
    bigint id PK
    bigint model_id
    text token_type
    bool is_prompt
    double base_rate
    json customization
    text _dlt_load_id
    text _dlt_id UK
}
    evaluators{
    bigint id PK
    text name
    text description
    json metadata
    text kind
    bigint user_id
    timestamp created_at
    text _dlt_load_id
    text _dlt_id UK
}
    builtin_evaluators{
    bigint id PK
    text kind
    text key
    json input_schema
    json output_configs
    timestamp synced_at
    text _dlt_load_id
    text _dlt_id UK
}
    dataset_examples{
    bigint id PK
    bigint dataset_id
    bigint span_rowid
    timestamp created_at
    text _dlt_load_id
    text _dlt_id UK
}
    datasets{
    bigint id PK
    text name
    text description
    json metadata
    timestamp created_at
    timestamp updated_at
    bigint user_id
    text _dlt_load_id
    text _dlt_id UK
}
    spans{
    bigint id PK
    bigint trace_rowid
    text span_id
    text parent_id
    text name
    text span_kind
    timestamp start_time
    timestamp end_time
    json attributes
    json events
    text status_code
    text status_message
    bigint cumulative_error_count
    bigint cumulative_llm_token_count_prompt
    bigint cumulative_llm_token_count_completion
    bigint llm_token_count_prompt
    bigint llm_token_count_completion
    text _dlt_load_id
    text _dlt_id UK
}
    traces{
    bigint id PK
    bigint project_rowid
    text trace_id
    timestamp start_time
    timestamp end_time
    bigint project_session_rowid
    text _dlt_load_id
    text _dlt_id UK
}
    project_sessions{
    bigint id PK
    text session_id
    bigint project_id
    timestamp start_time
    timestamp end_time
    text _dlt_load_id
    text _dlt_id UK
}
    dataset_example_revisions{
    bigint id PK
    bigint dataset_example_id
    bigint dataset_version_id
    json input
    json output
    json metadata
    text revision_kind
    timestamp created_at
    text _dlt_load_id
    text _dlt_id UK
}
    dataset_versions{
    bigint id PK
    bigint dataset_id
    text description
    json metadata
    timestamp created_at
    bigint user_id
    text _dlt_load_id
    text _dlt_id UK
}
    span_annotations{
    bigint id PK
    bigint span_rowid
    text name
    text label
    double score
    text explanation
    json metadata
    text annotator_kind
    timestamp created_at
    timestamp updated_at
    bigint user_id
    text identifier
    text source
    text _dlt_load_id
    text _dlt_id UK
}
    project_annotation_configs{
    bigint id PK
    bigint project_id
    bigint annotation_config_id
    text _dlt_load_id
    text _dlt_id UK
}
    annotation_configs{
    bigint id PK
    text name
    json config
    text _dlt_load_id
    text _dlt_id UK
}
    span_costs{
    bigint id PK
    bigint span_rowid
    bigint trace_rowid
    bigint model_id
    timestamp span_start_time
    double total_cost
    double total_tokens
    double prompt_cost
    double prompt_tokens
    double completion_cost
    double completion_tokens
    text _dlt_load_id
    text _dlt_id UK
}
    span_cost_details{
    bigint id PK
    bigint span_cost_id
    text token_type
    bool is_prompt
    double cost
    double tokens
    double cost_per_token
    text _dlt_load_id
    text _dlt_id UK
}
    users ||--|{ user_roles : ""
    projects ||--|{ project_trace_retention_policies : ""
    token_prices ||--|{ generative_models : ""
    evaluators ||--|{ users : ""
    builtin_evaluators ||--|{ evaluators : ""
    dataset_examples ||--|{ datasets : ""
    dataset_examples ||--|{ spans : ""
    datasets ||--|{ users : ""
    spans ||--|{ traces : ""
    traces ||--|{ projects : ""
    traces ||--|{ project_sessions : ""
    project_sessions ||--|{ projects : ""
    dataset_example_revisions ||--|{ dataset_versions : ""
    dataset_example_revisions ||--|{ dataset_examples : ""
    dataset_versions ||--|{ datasets : ""
    dataset_versions ||--|{ users : ""
    span_annotations ||--|{ users : ""
    span_annotations ||--|{ spans : ""
    project_annotation_configs ||--|{ projects : ""
    project_annotation_configs ||--|{ annotation_configs : ""
    span_costs ||--|{ traces : ""
    span_costs ||--|{ spans : ""
    span_costs ||--|{ generative_models : ""
    span_cost_details ||--|{ span_costs : ""
```

"""

import dlt
from dlt.sources.sql_database import sql_database


@dlt.source
def arize_phoenix_source(credentials=dlt.secrets.value):
    """Returns all Arize Phoenix tables as a dlt source.

    Reads directly from Phoenix's relational backend (PostgreSQL or SQLite) using
    dlt's built-in `sql_database` source. Foreign key relationships are resolved
    automatically so destination tables reference each other correctly.

    The primary tables of interest are:

    - **spans**: Every OTEL span recorded by Phoenix — the core unit of telemetry.
      Each row represents one span with timing, attributes, status, and token counts.
    - **traces**: Groups of related spans sharing the same `trace_id`.
    - **projects**: Phoenix projects that spans are assigned to.
    - **span_annotations**: Human or model-generated evaluations attached to spans
      (e.g. relevance score, hallucination label).
    - **trace_annotations**: Evaluations at the trace level.
    - **datasets / dataset_examples**: Curated examples used in prompt playgrounds
      or regression test suites.

    Args:
        credentials: SQLAlchemy-compatible connection string or credential dict,
            loaded from `secrets.toml`. Supports both PostgreSQL and SQLite URIs.

    Returns:
        A list of dlt resources — one per Phoenix database table.
    """
    return sql_database(
        credentials=credentials,
        resolve_foreign_keys=True,
    )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="arize_phoenix",
        # can be configure to any dlt destination
        destination="duckdb",
    )
    load_info = pipeline.run(arize_phoenix_source())
    print(load_info)
