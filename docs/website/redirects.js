// Redirect rules shared between the Cloudflare worker (worker.ts) and the
// post-build verification script (scripts/verify-redirects.js).

/** @type {Array<{from: string, to: string}>} */
const REDIRECTS = [
    // basic root redirects
    {
        // NOTE: We only ever hit the root path on dev previews, so we can redirect to the devel version
        from: "/",
        to: "/docs/devel/intro"
    },
    {
        from: "/docs",
        to: "/docs/intro"
    },
    {
        from: "/docs/",
        to: "/docs/intro"
    },
    {
        from: "/docs/hub",
        to: "/docs/hub/intro"
    },

    // docs section redirects
    {
        from: "/docs/getting-started",
        to: "/docs/intro/"
    },
    {
        from: "/docs/dlt-ecosystem",
        to: "/docs/dlt-ecosystem/verified-sources/"
    },
    {
        from: "/docs/general-usage/credentials/config_providers",
        to: "/docs/general-usage/credentials/setup/"
    },
    {
        from: "/docs/general-usage/credentials/configuration",
        to: "/docs/general-usage/credentials/setup/"
    },
    {
        from: "/docs/general-usage/credentials/config_specs",
        to: "/docs/general-usage/credentials/complex_types/"
    },

    // tutorial redirects
    {
        from: "/docs/tutorial/intro",
        to: "/docs/tutorial/load-data-from-an-api/"
    },
    {
        from: "/docs/tutorial/grouping-resources",
        to: "/docs/tutorial/load-data-from-an-api/"
    },

    // reference + misc redirects
    {
        from: "/docs/telemetry",
        to: "/docs/reference/telemetry/"
    },
    {
        from: "/docs/walkthroughs",
        to: "/docs/intro/"
    },
    {
        from: "/docs/visualizations",
        to: "/docs/general-usage/dataset-access/"
    },

    // dlt+ redirect
    {
        from: "/docs/hub",
        to: "/docs/hub/intro"
    },

    // top-404 redirects
    {
        from: "/docs/dlt-ecosystem/llm-tooling/cursor-restapi",
        to: "/docs/dlt-ecosystem/llm-tooling/llm-native-workflow"
    },
    {
        from: "/docs/general-usage/filesystem",
        to: "/docs/dlt-ecosystem/verified-sources/filesystem/"
    },
    {
        from: "/docs/walkthroughs/",
        to: "/docs/intro/"
    },
    {
        from: "/docs/dlt-ecosystem/verified-sources/rest_api/reference",
        to: "/docs/dlt-ecosystem/verified-sources/rest_api"
    },

    // renamed / relocated pages
    {
        from: "/docs/walkthroughs/load-data-from-an-api",
        to: "/docs/tutorial/load-data-from-an-api"
    },
    {
        from: "/docs/load-data-from-an-api",
        to: "/docs/tutorial/load-data-from-an-api"
    },
    {
        from: "/docs/destinations/snowflake",
        to: "/docs/dlt-ecosystem/destinations/snowflake"
    },
    {
        from: "/docs/destinations/duckdb",
        to: "/docs/dlt-ecosystem/destinations/duckdb"
    },
    {
        from: "/docs/pipelines/salesforce",
        to: "/docs/dlt-ecosystem/verified-sources/salesforce"
    },
    {
        from: "/docs/dlt-ecosystem/verified-sources/stripe_analytics",
        to: "/docs/dlt-ecosystem/verified-sources/stripe"
    },
    {
        from: "/docs/dlt-ecosystem/transformations/pandas",
        to: "/docs/dlt-ecosystem/transformations/python"
    },
    {
        from: "/docs/getting-started/build-a-data-pipeline",
        to: "/docs/build-a-pipeline-tutorial"
    },

    // api_reference paths gained a /dlt/ prefix
    {
        from: "/docs/api_reference/extract/resource",
        to: "/docs/api_reference/dlt/extract/resource"
    },
    {
        from: "/docs/api_reference/common/configuration/specs/base_configuration",
        to: "/docs/api_reference/dlt/common/configuration/specs/base_configuration"
    },
    {
        from: "/docs/hub/reference",
        to: "/docs/hub/intro"
    },
    {
        from: "/docs/general-usage/connectors",
        to: "/docs/dlt-ecosystem/verified-sources/"
    },
    {
        from: "/docs/api_reference/pipeline/configuration",
        to: "/docs/general-usage/credentials/"
    },
    {
        from: "/docs/walkthroughs/grouping-resources",
        to: "/docs/general-usage/source"
    },
    {
        from: "/docs/general-usage/dlt",
        to: "/docs/intro"
    },
];

module.exports = REDIRECTS;
