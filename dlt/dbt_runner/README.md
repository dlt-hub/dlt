https://github.com/davidgasquez/kubedbt
https://discourse.getdbt.com/t/running-dbt-in-kubernetes/92
https://github.com/godatadriven/pytest-dbt-core
https://github.com/great-expectations/great_expectations

https://github.com/fal-ai/fal (attach python scripts to models)

https://blog.getdbt.com/how-great-data-teams-test-their-data-models/

PG_DATABASE_NAME=chat_analytics_rasa PG_PASSWORD=8P5gyDPNo9zo582rQG6a PG_USER=loader PG_HOST=3.66.204.141 PG_PORT=5439 dbt list --profiles-dir . --vars '{source_schema_prefix: "unk"}' --resource-type test -s source:*

https://docs.getdbt.com/reference/node-selection/test-selection-examples


# list tests with selectors

PG_DATABASE_NAME=chat_analytics_rasa PG_PASSWORD=8P5gyDPNo9zo582rQG6a PG_USER=loader PG_HOST=3.66.204.141 PG_PORT=5439 dbt list --profiles-dir . --vars '{source_schema_prefix: "unk"}' --resource-type test -s views