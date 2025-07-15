"""Generic API Source"""

from copy import deepcopy
from typing import Any, Dict, List, Optional, cast, Union
import graphlib
from requests.auth import AuthBase

import dlt
from dlt.common.validation import validate_dict
from dlt.common import jsonpath
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TSchemaContract
from dlt.common.utils import exclude_keys
from dlt.common.exceptions import ValueErrorWithKnownValues

from dlt.extract import Incremental, DltResource, DltSource, decorators

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.rest_client.auth import (
    HttpBasicAuth,
    BearerTokenAuth,
    APIKeyAuth,
    AuthConfigBase,
    OAuth2ClientCredentials,
)
from dlt.sources.helpers.rest_client.typing import HTTPMethodBasic
from dlt.sources.helpers.rest_client.redaction import SENSITIVE_PARAMS
from .typing import (
    AuthConfig,
    ClientConfig,
    EndpointResourceBase,
    ResolvedParam,
    ResolveParamConfig,
    Endpoint,
    EndpointResource,
    IncrementalParamConfig,
    RESTAPIConfig,
    ParamBindType,
    ProcessingSteps,
)
from .config_setup import (
    IncrementalParam,
    create_auth,
    create_paginator,
    build_resource_dependency_graph,
    paginate_dependent_resource,
    paginate_resource,
    process_parent_data_item,
    setup_incremental_object,
    create_response_hooks,
    expand_placeholders,
    convert_incremental_values,
)
from .utils import check_connection  # noqa: F401

PARAM_TYPES: List[ParamBindType] = ["incremental", "resolve"]
MIN_SECRET_MASKING_LENGTH = 3
SENSITIVE_KEYS: List[str] = list(SENSITIVE_PARAMS) + ["username", "client_id"]


@decorators.source
def rest_api(
    client: ClientConfig = dlt.config.value,
    resources: List[Union[str, EndpointResource, DltResource]] = dlt.config.value,
    resource_defaults: Optional[EndpointResourceBase] = None,
) -> List[DltResource]:
    """Creates and configures a REST API source with default settings"""
    return rest_api_resources(
        {
            "client": client,
            "resources": resources,
            "resource_defaults": resource_defaults,
        }
    )


def rest_api_source(
    config: RESTAPIConfig,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    schema_contract: TSchemaContract = None,
    parallelized: bool = False,
) -> DltSource:
    """Creates and configures a REST API source for data extraction.

    Args:
        config (RESTAPIConfig): Configuration for the REST API source.
        name (str, optional): Name of the source.
        section (str, optional): Section of the configuration file.
        max_table_nesting (int, optional): Maximum depth of nested table above which
            the remaining nodes are loaded as structs or JSON.
        root_key (bool, optional): Enables merging on all resources by propagating
            root foreign key to child tables. This option is most useful if you
            plan to change write disposition of a resource to disable/enable merge.
            Defaults to False.
        schema (Schema, optional): An explicit `Schema` instance to be associated
            with the source. If not present, `dlt` creates a new `Schema` object
            with provided `name`. If such `Schema` already exists in the same
            folder as the module containing the decorated function, such schema
            will be loaded from file.
        schema_contract (TSchemaContract, optional): Schema contract settings
            that will be applied to this resource.
        parallelized (bool, optional): If `True`, resource generators will be
            extracted in parallel with other resources. Transformers that return items are also parallelized.
            Non-eligible resources are ignored. Defaults to `False` which preserves resource settings.

    Returns:
        DltSource: A configured dlt source.

    Example:
        pokemon_source = rest_api_source({
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                "paginator": "json_link",
            },
            "resources": [
                {
                    "name": "pokemon",
                    "endpoint": {
                        "path": "pokemon",
                        "params": {
                            "limit": 100,
                        },
                    },
                    "primary_key": "id",
                }
            ]
        })
    """
    # TODO: this must be removed when TypedDicts are supported by resolve_configuration
    #   so secrets values are bound BEFORE validation. validation will happen during the resolve process
    _validate_config(config)
    decorated = rest_api.clone(
        name=name,
        section=section,
        max_table_nesting=max_table_nesting,
        root_key=root_key,
        schema=schema,
        schema_contract=schema_contract,
        parallelized=parallelized,
    )

    return decorated(**config)


def rest_api_resources(config: RESTAPIConfig) -> List[DltResource]:
    """Creates a list of resources from a REST API configuration.

    Args:
        config (RESTAPIConfig): Configuration for the REST API source.

    Returns:
        List[DltResource]: List of dlt resources.

    Example:
        github_source = rest_api_resources({
            "client": {
                "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
                "auth": {
                    "token": dlt.secrets["token"],
                },
            },
            "resource_defaults": {
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "params": {
                        "per_page": 100,
                    },
                },
            },
            "resources": [
                {
                    "name": "issues",
                    "endpoint": {
                        "path": "issues",
                        "params": {
                            "sort": "updated",
                            "direction": "desc",
                            "state": "open",
                            "since": "{incremental.start_value}",
                        },
                        "incremental": {
                            "cursor_path": "updated_at",
                            "initial_value": "2024-01-25T11:21:28Z",
                        },
                    },
                },
                {
                    "name": "issue_comments",
                    "endpoint": {
                        "path": "issues/{resources.issues.number}/comments",
                    },
                },
            ],
        })
    """

    _validate_config(config)

    client_config = config["client"]
    resource_defaults = config.get("resource_defaults") or {}
    resource_list = config["resources"]

    (
        dependency_graph,
        endpoint_resource_map,
        resolved_param_map,
    ) = build_resource_dependency_graph(
        resource_defaults,
        resource_list,
    )

    resources = create_resources(
        client_config,
        dependency_graph,
        endpoint_resource_map,
        resolved_param_map,
    )

    return list(resources.values())


def create_resources(
    client_config: ClientConfig,
    dependency_graph: graphlib.TopologicalSorter,  # type: ignore[type-arg]
    endpoint_resource_map: Dict[str, Union[EndpointResource, DltResource]],
    resolved_param_map: Dict[str, Optional[List[ResolvedParam]]],
) -> Dict[str, DltResource]:
    resources = {}

    for resource_name in dependency_graph.static_order():
        resource_name = cast(str, resource_name)
        endpoint_resource = endpoint_resource_map[resource_name]
        if isinstance(endpoint_resource, DltResource):
            resources[resource_name] = endpoint_resource
            continue

        endpoint_config = cast(Endpoint, endpoint_resource["endpoint"])
        request_params = endpoint_config.get("params", {})
        request_json = endpoint_config.get("json")
        request_headers = endpoint_config.get("headers")
        paginator = create_paginator(endpoint_config.get("paginator"))
        processing_steps = endpoint_resource.pop("processing_steps", [])

        resolved_params: List[ResolvedParam] = resolved_param_map[resource_name]

        include_from_parent: List[str] = endpoint_resource.get("include_from_parent", [])
        if not resolved_params and include_from_parent:
            raise ValueError(
                f"Resource `{resource_name}` has `include_from_parent` but is not "
                "dependent on another resource"
            )
        _validate_param_type(request_params)
        (
            incremental_object,
            incremental_param,
            incremental_cursor_transform,
        ) = setup_incremental_object(request_params, endpoint_config.get("incremental"))

        client = RESTClient(
            base_url=client_config["base_url"],
            headers=client_config.get("headers"),
            auth=create_auth(endpoint_config.get("auth", client_config.get("auth"))),
            paginator=create_paginator(client_config.get("paginator")),
            session=client_config.get("session"),
        )

        hooks = create_response_hooks(endpoint_config.get("response_actions"))

        resource_kwargs = exclude_keys(endpoint_resource, {"endpoint", "include_from_parent"})

        def process(
            resource: DltResource,
            processing_steps: List[ProcessingSteps],
        ) -> Any:
            for step in processing_steps:
                if "filter" in step:
                    resource.add_filter(step["filter"])
                if "map" in step:
                    resource.add_map(step["map"])
            return resource

        if resolved_params is None:
            resources[resource_name] = dlt.resource(
                paginate_resource,
                **resource_kwargs,  # TODO: implement typing.Unpack
            )(
                method=endpoint_config.get("method", "GET"),
                path=endpoint_config.get("path"),
                headers=request_headers,
                params=request_params,
                json=request_json,
                paginator=paginator,
                data_selector=endpoint_config.get("data_selector"),
                hooks=hooks,
                client=client,
                incremental_object=incremental_object,
                incremental_param=incremental_param,
                incremental_cursor_transform=incremental_cursor_transform,
            )

            resources[resource_name] = process(resources[resource_name], processing_steps)

        else:
            first_param = resolved_params[0]
            predecessor = resources[first_param.resolve_config["resource"]]

            base_params = exclude_keys(request_params, {x.param_name for x in resolved_params})

            resources[resource_name] = dlt.resource(  # type: ignore[call-overload]
                paginate_dependent_resource,
                data_from=predecessor,
                **resource_kwargs,  # TODO: implement typing.Unpack
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                headers=request_headers,
                params=base_params,
                json=request_json,
                paginator=paginator,
                data_selector=endpoint_config.get("data_selector"),
                hooks=hooks,
                client=client,
                resolved_params=resolved_params,
                include_from_parent=include_from_parent,
                incremental_object=incremental_object,
                incremental_param=incremental_param,
                incremental_cursor_transform=incremental_cursor_transform,
            )

            resources[resource_name] = process(resources[resource_name], processing_steps)

    return resources


def _validate_config(config: RESTAPIConfig) -> None:
    c = deepcopy(config)
    client_config = c.get("client")
    if client_config:
        auth = client_config.get("auth")
        if auth:
            _mask_secrets(auth)
    resources = c.get("resources", [])
    for resource in resources:
        if isinstance(resource, (str, DltResource)):
            continue
        if endpoint := resource.get("endpoint"):
            if not isinstance(endpoint, str):
                auth = endpoint.get("auth")
                if auth:
                    _mask_secrets(auth)

    validate_dict(RESTAPIConfig, c, path=".")


def _mask_secrets(auth_config: AuthConfig) -> AuthConfig:
    # skip AuthBase (derived from requests lib) or shorthand notation
    if (
        isinstance(auth_config, AuthBase)
        and not isinstance(auth_config, AuthConfigBase)
        or isinstance(auth_config, str)
    ):
        return auth_config

    has_sensitive_key = any(key in auth_config for key in SENSITIVE_KEYS)
    if (
        isinstance(
            auth_config,
            (APIKeyAuth, BearerTokenAuth, HttpBasicAuth, OAuth2ClientCredentials),
        )
        or has_sensitive_key
    ):
        return _mask_secrets_dict(auth_config)
    # Here, we assume that OAuth2 and other custom classes that don't implement __get__()
    # also don't print secrets in __str__()
    # TODO: call auth_config.mask_secrets() when that is implemented
    return auth_config


def _mask_secrets_dict(auth_config: AuthConfig) -> AuthConfig:
    for sensitive_key in SENSITIVE_KEYS:
        try:
            auth_config[sensitive_key] = _mask_secret(auth_config[sensitive_key])  # type: ignore[literal-required, index]
        except KeyError:
            continue
    return auth_config


def _mask_secret(secret: Optional[str]) -> str:
    if secret is None:
        return "None"
    if len(secret) < MIN_SECRET_MASKING_LENGTH:
        return "*****"
    return f"{secret[0]}*****{secret[-1]}"


def _validate_param_type(
    request_params: Dict[str, Union[ResolveParamConfig, IncrementalParamConfig, Any]],
) -> None:
    for param_name, value in request_params.items():
        if isinstance(value, dict) and value.get("type") not in PARAM_TYPES:
            raise ValueErrorWithKnownValues(f"{param_name}['type']", value.get("type"), PARAM_TYPES)
