import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {
        "base_url": "https://jaffle-shop.scalevector.ai/api/v1/",
    },
    "resource_defaults": {
        "write_disposition": "merge",
        "endpoint": {
            "params": {
                "page_size": 5000  # Increased page size for better performance
            }
        },
    },
    "resources": [
        {
            "name": "customers",
            "endpoint": {
                "path": "customers",
                "params": {"page_size": 5000},
                "paginator": {"type": "header_link"},  # Changed to header_link pagination
            },
        },
        {
            "name": "orders",
            "endpoint": {
                "path": "orders",
                "params": {"page_size": 5000},
                "paginator": {"type": "header_link"},
            },
        },
        {
            "name": "items",
            "endpoint": {
                "path": "items",
                "params": {"page_size": 5000},
                "paginator": {"type": "header_link"},
            },
        },
        {
            "name": "products",
            "endpoint": {
                "path": "products",
                "params": {"page_size": 5000},
                "paginator": {"type": "header_link"},
            },
        },
        {
            "name": "supplies",
            "endpoint": {
                "path": "supplies",
                "params": {"page_size": 5000},
                "paginator": {"type": "header_link"},
            },
        },
        {
            "name": "stores",
            "endpoint": {
                "path": "stores",
                "params": {"page_size": 5000},
                "paginator": {"type": "header_link"},
            },
        },
        {
            "name": "row_counts",
            "endpoint": {
                "path": "row-counts",
                "paginator": {
                    "type": "single_page"
                },  # Keep single_page for row counts as it's likely a summary endpoint
            },
        },
    ],
})

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_rest",
        destination="postgres",
        dataset_name="jaffle_shop_data",
        progress="log",
    )
    load_info = pipeline.run(source)
    print(load_info)
