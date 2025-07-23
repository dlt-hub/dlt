import warnings
from copy import copy
from typing import (
    Generator,
    Type,
    Any,
    Dict,
    Set,
    Tuple,
    List,
    Iterable,
    Optional,
    Union,
    Callable,
    cast,
    NamedTuple,
)
import graphlib
import string
from requests import Response

from dlt.common import logger
from dlt.common.configuration import resolve_configuration
from dlt.common.exceptions import TypeErrorWithKnownTypes, ValueErrorWithKnownValues
from dlt.common.schema.utils import merge_columns
from dlt.common.utils import update_dict_nested, exclude_keys
from dlt.common.typing import add_value_to_literal
from dlt.common import jsonpath

from dlt.extract.incremental import Incremental
from dlt.extract.utils import ensure_table_schema_columns

from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    SinglePagePaginator,
    JSONLinkPaginator,
    HeaderLinkPaginator,
    HeaderCursorPaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)

from dlt.sources.helpers.rest_client.detector import single_entity_path
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.helpers.rest_client.auth import (
    AuthConfigBase,
    HttpBasicAuth,
    BearerTokenAuth,
    APIKeyAuth,
    OAuth2ClientCredentials,
)
from dlt.sources.helpers.rest_client.client import RESTClient, raise_for_status

from dlt.extract.resource import DltResource
from dlt.sources.helpers.rest_client.typing import HTTPMethodBasic

from .typing import (
    EndpointResourceBase,
    AuthConfig,
    IncrementalConfig,
    PaginatorConfig,
    ResolvedParam,
    ResponseAction,
    ResponseActionDict,
    Endpoint,
    EndpointResource,
    AuthType,
    PaginatorType,
)


PAGINATOR_MAP: Dict[str, Type[BasePaginator]] = {
    "json_link": JSONLinkPaginator,
    "json_response": (
        JSONLinkPaginator
    ),  # deprecated. Use json_link instead. Will be removed in upcoming release
    "header_link": HeaderLinkPaginator,
    "header_cursor": HeaderCursorPaginator,
    "auto": None,
    "single_page": SinglePagePaginator,
    "cursor": JSONResponseCursorPaginator,
    "offset": OffsetPaginator,
    "page_number": PageNumberPaginator,
}

AUTH_MAP: Dict[str, Type[AuthConfigBase]] = {
    "bearer": BearerTokenAuth,
    "api_key": APIKeyAuth,
    "http_basic": HttpBasicAuth,
    "oauth2_client_credentials": OAuth2ClientCredentials,
}


class IncrementalParam(NamedTuple):
    start: Optional[str]
    end: Optional[str]


class ProcessedParentData(NamedTuple):
    path: str
    headers: Optional[Dict[str, Any]]
    params: Dict[str, Any]
    json: Optional[Dict[str, Any]]
    parent_record: Dict[str, Any]


class DirectKeyFormatter(string.Formatter):
    def get_field(self, field_name: str, args: Any, kwargs: Any) -> Any:
        if field_name in kwargs:
            return kwargs[field_name], field_name
        return super().get_field(field_name, args, kwargs)


def register_paginator(
    paginator_name: str,
    paginator_class: Type[BasePaginator],
) -> None:
    if not issubclass(paginator_class, BasePaginator):
        raise ValueError(
            f"Invalid paginator: `{paginator_class.__name__}`. "
            "Your custom paginator has to be a subclass of `BasePaginator`"
        )
    PAGINATOR_MAP[paginator_name] = paginator_class
    add_value_to_literal(PaginatorType, paginator_name)


def get_paginator_class(paginator_name: str) -> Type[BasePaginator]:
    try:
        return PAGINATOR_MAP[paginator_name]
    except KeyError:
        raise ValueErrorWithKnownValues(
            "paginator_name", paginator_name, list(PAGINATOR_MAP.keys())
        )


def create_paginator(
    paginator_config: Optional[PaginatorConfig],
) -> Optional[BasePaginator]:
    if isinstance(paginator_config, BasePaginator):
        return paginator_config

    if isinstance(paginator_config, str):
        paginator_class = get_paginator_class(paginator_config)
        try:
            # `auto` has no associated class in `PAGINATOR_MAP`
            return paginator_class() if paginator_class else None
        except TypeError:
            raise ValueError(
                f"Paginator `{paginator_config}` requires arguments to create an instance. Use"
                f" `{paginator_class}` instance instead."
            )

    if isinstance(paginator_config, dict):
        paginator_type = paginator_config.get("type", "auto")
        paginator_class = get_paginator_class(paginator_type)
        return (
            paginator_class(**exclude_keys(paginator_config, {"type"})) if paginator_class else None
        )

    return None


def register_auth(
    auth_name: str,
    auth_class: Type[AuthConfigBase],
) -> None:
    if not issubclass(auth_class, AuthConfigBase):
        raise ValueError(
            f"Invalid auth: `{auth_class.__name__}`. "
            "Your custom auth has to be a subclass of `AuthConfigBase`"
        )
    AUTH_MAP[auth_name] = auth_class

    add_value_to_literal(AuthType, auth_name)


def get_auth_class(auth_type: str) -> Type[AuthConfigBase]:
    try:
        return AUTH_MAP[auth_type]
    except KeyError:
        raise ValueErrorWithKnownValues("auth_type", auth_type, list(AUTH_MAP.keys()))


def create_auth(auth_config: Optional[AuthConfig]) -> Optional[AuthConfigBase]:
    auth: AuthConfigBase = None
    if isinstance(auth_config, AuthConfigBase):
        auth = auth_config

    if isinstance(auth_config, str):
        auth_class = get_auth_class(auth_config)
        auth = auth_class()

    if isinstance(auth_config, dict):
        auth_type = auth_config.get("type", "bearer")
        auth_class = get_auth_class(auth_type)
        auth = auth_class.from_init_value(exclude_keys(auth_config, {"type"}))

    if auth_config is not None and auth is None:
        raise ValueError(
            f"Incorrect auth object type '{type(auth_config).__name__}'. "
            "Expected str (auth type), dict (auth config), an instance of AuthConfigBase, or None."
        )

    if auth and not auth.__is_resolved__:
        # TODO: provide explicitly (non-default) values as explicit explicit_value=dict(auth)
        # this will resolve auth which is a configuration using current section context
        auth = resolve_configuration(auth, accept_partial=False)

    return auth


def setup_incremental_object(
    request_params: Dict[str, Any],
    incremental_config: Optional[IncrementalConfig] = None,
) -> Tuple[Optional[Incremental[Any]], Optional[IncrementalParam], Optional[Callable[..., Any]]]:
    incremental_params: List[str] = []
    for param_name, param_config in request_params.items():
        if (
            isinstance(param_config, dict)
            and param_config.get("type") == "incremental"
            or isinstance(param_config, Incremental)
        ):
            incremental_params.append(param_name)
    if len(incremental_params) > 1:
        raise ValueError(
            "Only a single incremental parameter is allower per endpoint. Found parameters: "
            f"`{incremental_params}`"
        )
    convert: Optional[Callable[..., Any]]
    for param_name, param_config in request_params.items():
        if isinstance(param_config, Incremental):
            if param_config.end_value is not None:
                raise ValueError(
                    "Only `initial_value` is allowed in the configuration of param:"
                    f" `{param_name}`. "
                    "To set `end_value` too use the incremental configuration at the resource"
                    " level. "
                    "See https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic#incremental-loading"
                )
            return param_config, IncrementalParam(start=param_name, end=None), None
        if isinstance(param_config, dict) and param_config.get("type") == "incremental":
            if param_config.get("end_value") or param_config.get("end_param"):
                raise ValueError(
                    "Only `start_param` and `initial_value` are allowed in the configuration of"
                    f" param: `{param_name}`. "
                    "To set `end_value` too use the incremental configuration at the resource"
                    " level. "
                    "See https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic#incremental-loading"
                )
            convert = parse_convert_or_deprecated_transform(param_config)

            config = exclude_keys(param_config, {"type", "convert", "transform"})
            # TODO: implement param type to bind incremental to
            return (
                Incremental(**config),
                IncrementalParam(start=param_name, end=None),
                convert,
            )
    if incremental_config:
        convert = parse_convert_or_deprecated_transform(incremental_config)
        config = exclude_keys(
            incremental_config, {"start_param", "end_param", "convert", "transform"}
        )
        return (
            Incremental(**config),
            IncrementalParam(
                start=incremental_config.get("start_param"),
                end=incremental_config.get("end_param"),
            ),
            convert,
        )

    return None, None, None


def parse_convert_or_deprecated_transform(
    config: Union[IncrementalConfig, Dict[str, Any]],
) -> Optional[Callable[..., Any]]:
    convert = config.get("convert", None)
    deprecated_transform = config.get("transform", None)
    if deprecated_transform:
        warnings.warn(
            "The key `transform` is deprecated in the incremental configuration and it will be"
            " removed. Use `convert` instead",
            DeprecationWarning,
            stacklevel=2,
        )
        convert = deprecated_transform
    return convert


def make_parent_key_name(resource_name: str, field_name: str) -> str:
    return f"_{resource_name}_{field_name}"


def _filter_resource_expressions(expressions: Set[str]) -> Set[str]:
    return {x for x in expressions if x.startswith("resources.")}


def build_resource_dependency_graph(
    resource_defaults: EndpointResourceBase,
    resource_list: List[Union[str, EndpointResource, DltResource]],
) -> Tuple[  # type: ignore[type-arg]
    graphlib.TopologicalSorter,
    Dict[str, Union[EndpointResource, DltResource]],
    Dict[str, Optional[List[ResolvedParam]]],
]:
    dependency_graph: graphlib.TopologicalSorter = graphlib.TopologicalSorter()  # type: ignore[type-arg]
    resolved_param_map: Dict[str, Optional[List[ResolvedParam]]] = {}
    endpoint_resource_map = expand_and_index_resources(resource_list, resource_defaults)
    # create dependency graph
    for resource_name, endpoint_resource in endpoint_resource_map.items():
        if isinstance(endpoint_resource, DltResource):
            dependency_graph.add(resource_name)
            resolved_param_map[resource_name] = None
            continue
        assert isinstance(endpoint_resource["endpoint"], dict)
        # find resolved parameters to connect dependent resources
        resolved_params = _find_resolved_params(endpoint_resource["endpoint"])

        available_contexts = _get_available_contexts(endpoint_resource["endpoint"])

        # Find more resolved params in path
        # Ignore params that are not in available_contexts for backward compatibility with
        # resolved params in path: these are validated in _bind_path_params
        path_expressions = _find_expressions(
            endpoint_resource["endpoint"]["path"], available_contexts
        )

        # Find all expressions in params, json, or header, but error if any of them is not in available_contexts
        params_expressions = _find_expressions(endpoint_resource["endpoint"].get("params", {}))
        _raise_if_any_not_in(params_expressions, available_contexts, message="params")

        json_expressions = _find_expressions(endpoint_resource["endpoint"].get("json", {}))
        _raise_if_any_not_in(json_expressions, available_contexts, message="json")

        headers_expressions = _find_expressions(endpoint_resource["endpoint"].get("headers", {}))
        _raise_if_any_not_in(headers_expressions, available_contexts, message="headers")

        resolved_params += _expressions_to_resolved_params(
            _filter_resource_expressions(
                path_expressions | params_expressions | json_expressions | headers_expressions
            )
        )

        # set of resources in resolved params
        named_resources = {rp.resolve_config["resource"] for rp in resolved_params}

        if len(named_resources) > 1:
            raise ValueError(
                f"Multiple parent resources for resource `{resource_name}` with params"
                f" `{resolved_params}`"
            )
        elif len(named_resources) == 1:
            # validate the first parameter (note the resource is the same for all params)
            first_param = resolved_params[0]
            predecessor = first_param.resolve_config["resource"]
            if predecessor not in endpoint_resource_map:
                raise ValueError(
                    f"A dependent resource `{resource_name}` refers to non existing parent resource"
                    f" `{predecessor}` on param `{first_param}`"
                )

            dependency_graph.add(resource_name, predecessor)
            resolved_param_map[resource_name] = resolved_params
        else:
            dependency_graph.add(resource_name)
            resolved_param_map[resource_name] = None

    return dependency_graph, endpoint_resource_map, resolved_param_map


def expand_and_index_resources(
    resource_list: List[Union[str, EndpointResource, DltResource]],
    resource_defaults: EndpointResourceBase,
) -> Dict[str, Union[EndpointResource, DltResource]]:
    endpoint_resource_map: Dict[str, Union[EndpointResource, DltResource]] = {}
    for resource in resource_list:
        if isinstance(resource, DltResource):
            endpoint_resource_map[resource.name] = resource
            continue
        elif isinstance(resource, dict):
            # clone resource here, otherwise it needs to be cloned in several other places
            # note that this clones only dict structure, keeping all instances without deepcopy
            resource = update_dict_nested({}, resource)  # type: ignore

        endpoint_resource = _make_endpoint_resource(resource, resource_defaults)
        assert isinstance(endpoint_resource["endpoint"], dict)
        _setup_single_entity_endpoint(endpoint_resource["endpoint"])
        _bind_path_params(endpoint_resource)

        resource_name = endpoint_resource["name"]
        assert isinstance(
            resource_name, str
        ), f"Resource name must be a string, got {type(resource_name)}"

        if resource_name in endpoint_resource_map:
            raise ValueError(f"Resource `{resource_name}` is already defined.")
        endpoint_resource_map[resource_name] = endpoint_resource

    return endpoint_resource_map


def _make_endpoint_resource(
    resource: Union[str, EndpointResource], default_config: EndpointResourceBase
) -> EndpointResource:
    """
    Creates an EndpointResource object based on the provided resource
    definition and merges it with the default configuration.

    This function supports defining a resource in multiple formats:
    - As a string: The string is interpreted as both the resource name
        and its endpoint path.
    - As a dictionary: The dictionary must include `name` and `endpoint`
        keys. The `endpoint` can be a string representing the path,
        or a dictionary for more complex configurations. If the `endpoint`
        is missing the `path` key, the resource name is used as the `path`.
    """
    if isinstance(resource, str):
        resource = {"name": resource, "endpoint": {"path": resource}}
        return _merge_resource_endpoints(default_config, resource)

    if "endpoint" in resource:
        if isinstance(resource["endpoint"], str):
            resource["endpoint"] = {"path": resource["endpoint"]}
    else:
        # endpoint is optional
        resource["endpoint"] = {}

    if "path" not in resource["endpoint"]:
        resource["endpoint"]["path"] = resource["name"]  # type: ignore

    return _merge_resource_endpoints(default_config, resource)


def _bind_path_params(resource: EndpointResource) -> None:
    """Binds params declared in path to params available in `params`. Pops the
    bound params. Params of type `resolve` and `incremental` are skipped
    and bound later.
    """
    # TODO: Deprecate static params usage in path
    # TODO: and remove this function
    assert isinstance(resource["endpoint"], dict)  # type guard

    endpoint = resource["endpoint"]
    params = endpoint.get("params", {})
    path = endpoint["path"]

    resolve_params = [r.param_name for r in _find_resolved_params(endpoint)]

    available_contexts = _get_available_contexts(endpoint)

    new_path_segments = []

    for literal_text, field_name, _, _ in string.Formatter().parse(path):
        # Always add literal text
        new_path_segments.append(literal_text)

        if not field_name:
            # There's no placeholder here
            continue

        # If placeholder starts with a recognized context, leave it intact
        # e.g. "resources." or "incremental." or any future contexts
        if any(field_name.startswith(prefix + ".") for prefix in available_contexts):
            new_path_segments.append(f"{{{field_name}}}")
            continue

        # If it's a "resolve" param, skip binding here so it remains in the path
        # and can be processed later
        if field_name in resolve_params:
            # We insert a literal placeholder instead of substituting a value
            new_path_segments.append(f"{{{field_name}}}")
            # Remove from the list of resolve params so we don't complain about it later
            resolve_params.remove(field_name)
            continue

        # Otherwise, we attempt to bind a normal param from endpoint['params']
        if field_name not in params:
            # Does not have a dot in the field name: most likely should be a resolve param
            if "." not in field_name:
                raise ValueError(
                    f"The path '{path}' defined in resource '{resource['name']}' requires a param "
                    f"named '{field_name}', but it was not found in 'endpoint.params': {params}"
                )
            else:
                # Most likely mistyped placeholder context name
                raise ValueError(
                    f"The `{path=:}` defined in resource `{resource['name']}` contains a"
                    f" placeholder `{field_name}`. This placeholder is not a valid name."
                    " Valid names are: ['resources', 'incremental']."
                )

        if not isinstance(params[field_name], dict):
            # bind resolved param and pop it from endpoint
            value = params.pop(field_name)
            new_path_segments.append(str(value))
        else:
            param_type = params[field_name].get("type")
            if param_type != "resolve":
                raise ValueError(
                    f"The path `{path}` defined in resource `{resource['name']}` tries to bind"
                    f" param `{field_name}` with type `{param_type}`. Paths can only bind 'resolve'"
                    " type params."
                )

    if len(resolve_params) > 0:
        raise ValueError(
            f"Resource `{resource['name']}` defines resolve params `{resolve_params}` that are not"
            f" bound in path `{path}`. To reference parent resource in query params use syntax"
            " 'resources.<parent_resource>.<field>'"
        )

    resource["endpoint"]["path"] = "".join(new_path_segments)


def _setup_single_entity_endpoint(endpoint: Endpoint) -> Endpoint:
    """Tries to guess if the endpoint refers to a single entity and when detected:
    * if `data_selector` was not specified (or is None), "$" is selected
    * if `paginator` was not specified (or is None), SinglePagePaginator is selected

    Endpoint is modified in place and returned
    """
    # try to guess if list of entities or just single entity is returned
    if single_entity_path(endpoint["path"]):
        if endpoint.get("data_selector") is None:
            endpoint["data_selector"] = "$"
        if endpoint.get("paginator") is None:
            endpoint["paginator"] = SinglePagePaginator()
    return endpoint


def _find_resolved_params(endpoint_config: Endpoint) -> List[ResolvedParam]:
    """
    Find all resolved params in the endpoint configuration and return
    a list of ResolvedParam objects.

    Resolved params are of type ResolveParamConfig (bound param with a key "type" set to "resolve".)
    """
    return [
        ResolvedParam(key, value)  # type: ignore[arg-type]
        for key, value in endpoint_config.get("params", {}).items()
        if (isinstance(value, dict) and value.get("type") == "resolve")
    ]


def _action_type_unless_custom_hook(
    action_type: Optional[str], custom_hook: Optional[List[Callable[..., Any]]]
) -> Union[Tuple[str, Optional[List[Callable[..., Any]]]], Tuple[None, List[Callable[..., Any]]],]:
    if custom_hook:
        return (None, custom_hook)
    return (action_type, None)


def _handle_response_action(
    response: Response,
    action: ResponseAction,
) -> Union[
    Tuple[str, Optional[List[Callable[..., Any]]]],
    Tuple[None, List[Callable[..., Any]]],
    Tuple[None, None],
]:
    """
    Checks, based on the response, if the provided action applies.
    """
    content: str = response.text
    status_code = None
    content_substr = None
    action_type = None
    custom_hooks = None
    response_action = None
    if callable(action):
        custom_hooks = [action]
    else:
        action = cast(ResponseActionDict, action)
        status_code = action.get("status_code")
        content_substr = action.get("content")
        response_action = action.get("action")
        if isinstance(response_action, str):
            action_type = response_action
        elif callable(response_action):
            custom_hooks = [response_action]
        elif isinstance(response_action, list) and all(
            callable(action) for action in response_action
        ):
            custom_hooks = response_action
        else:
            raise TypeErrorWithKnownTypes(
                "action['action']", response_action, ["str", "Callable", "List[Callable]"]
            )

    if status_code is not None and content_substr is not None:
        if response.status_code == status_code and content_substr in content:
            return _action_type_unless_custom_hook(action_type, custom_hooks)

    elif status_code is not None:
        if response.status_code == status_code:
            return _action_type_unless_custom_hook(action_type, custom_hooks)

    elif content_substr is not None:
        if content_substr in content:
            return _action_type_unless_custom_hook(action_type, custom_hooks)

    elif status_code is None and content_substr is None and custom_hooks is not None:
        return (None, custom_hooks)

    return (None, None)


def _create_response_action_hook(
    response_action: ResponseAction,
) -> Callable[[Response, Any, Any], None]:
    def response_action_hook(response: Response, *args: Any, **kwargs: Any) -> None:
        """
        This is the hook executed by the requests library
        """
        (action_type, custom_hooks) = _handle_response_action(response, response_action)
        if custom_hooks:
            for hook in custom_hooks:
                hook(response)
        elif action_type == "ignore":
            logger.info(
                f"Ignoring response with code {response.status_code} and content '{response.text}'."
            )
            raise IgnoreResponseException

    return response_action_hook


def create_response_hooks(
    response_actions: Optional[List[ResponseAction]],
) -> Optional[Dict[str, Any]]:
    """Create response hooks based on the provided response actions. Note
    that if the error status code is not handled by the response actions,
    the default behavior is to raise an HTTP error.

    Example:
        def set_encoding(response, *args, **kwargs):
            response.encoding = 'windows-1252'
            return response

        def remove_field(response: Response, *args, **kwargs) -> Response:
            payload = response.json()
            for record in payload:
                record.pop("email", None)
            modified_content: bytes = json.dumps(payload).encode("utf-8")
            response._content = modified_content
            return response

        response_actions = [
            set_encoding,
            {"status_code": 404, "action": "ignore"},
            {"content": "Not found", "action": "ignore"},
            {"status_code": 200, "content": "some text", "action": "ignore"},
            {"status_code": 200, "action": remove_field},
        ]
        hooks = create_response_hooks(response_actions)
    """
    if response_actions:
        hooks = [_create_response_action_hook(action) for action in response_actions]
        fallback_hooks = [raise_for_status]
        return {"response": hooks + fallback_hooks}
    return None


def _find_expressions(
    content: Union[str, Dict[str, Any]],
    prefixes: Optional[Iterable[str]] = None,
) -> Set[str]:
    """Takes a string, dictionary, or nested structure and extracts expressions
    that start with any of the given prefixes. If prefixes is None, extracts all expressions.
    Recursively searches through dictionaries and lists to find expressions in string values.

    Args:
        content (Union[str, Dict[str, Any]]): A string, dictionary, or nested structure
            to search for expressions
        prefixes (Optional[Iterable[str]]): An iterable of strings that mark the beginning
            of expressions. If None, all expressions are included.

    Returns:
        Set[str]: Set of found expressions that match the prefix criteria (or all if no prefixes)

    Example:
        >>> _find_expressions("blog/{resources.blog.id}/comments", ["resources."])
        {"resources.blog.id"}
        >>> _find_expressions("blog/{resources.blog.id}/comments", None)
        {"resources.blog.id"}
        >>> _find_expressions("blog/{id}/comments", None)
        {"id"}
    """
    expressions = set()

    def recursive_search(value: Union[str, List[Any], Dict[str, Any]]) -> None:
        if isinstance(value, dict):
            for key, val in value.items():
                recursive_search(key)
                recursive_search(val)
        elif isinstance(value, list):
            for item in value:
                recursive_search(item)
        elif isinstance(value, str):
            e = [
                field_name
                for _, field_name, _, _ in string.Formatter().parse(value)
                if field_name
                and (prefixes is None or any(field_name.startswith(prefix) for prefix in prefixes))
            ]
            expressions.update(e)

    recursive_search(content)
    return expressions


def _expressions_to_resolved_params(expressions: Set[str]) -> List[ResolvedParam]:
    resolved_params = []
    # We assume that the expressions are in the format 'resources.<resource>.<field>'
    # and not more complex expressions
    for expression in expressions:
        parts = expression.strip().split(".", maxsplit=2)
        if len(parts) != 3:
            raise ValueError(
                f"Invalid definition of `{expression}`. Expected format:"
                " 'resources.<resource>.<field>'"
            )
        resolved_params.append(
            ResolvedParam(
                expression,
                {
                    "type": "resolve",
                    "resource": parts[1],
                    "field": parts[2],
                },
            )
        )
    return resolved_params


def process_parent_data_item(
    path: str,
    item: Dict[str, Any],
    resolved_params: List[ResolvedParam],
    headers: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    request_json: Optional[Dict[str, Any]] = None,
    include_from_parent: Optional[List[str]] = None,
    incremental: Optional[Incremental[Any]] = None,
    incremental_value_convert: Optional[Callable[..., Any]] = None,
) -> ProcessedParentData:
    params_values = collect_resolved_values(
        item, resolved_params, incremental, incremental_value_convert
    )
    expanded_path = expand_placeholders(path, params_values)
    expanded_headers = expand_placeholders(headers, params_values)
    expanded_params = expand_placeholders(params or {}, params_values)
    expanded_json = expand_placeholders(request_json, params_values, preserve_value_type=True)

    parent_resource_name = resolved_params[0].resolve_config["resource"]
    parent_record = build_parent_record(item, parent_resource_name, include_from_parent)

    return ProcessedParentData(
        path=expanded_path,
        headers=expanded_headers,
        params=expanded_params,
        json=expanded_json,
        parent_record=parent_record,
    )


def convert_incremental_values(
    incremental: Incremental[Any], convert: Callable[..., Any]
) -> Dict[str, Any]:
    values = {
        "initial_value": incremental.initial_value,
        "start_value": incremental.start_value,
        "last_value": incremental.last_value,
        "end_value": incremental.end_value,
    }
    return {
        f"incremental.{key}": convert(value) if value is not None else None
        for key, value in values.items()
    }


def collect_resolved_values(
    item: Dict[str, Any],
    resolved_params: List[ResolvedParam],
    incremental: Optional[Incremental[Any]],
    incremental_value_convert: Optional[Callable[..., Any]],
) -> Dict[str, Any]:
    """
    Collects field values from the parent item based on resolved_params
    and sets up incremental if present. Returns the resulting placeholders
    (params_values) and a ResourcesContext that may store `resources.<name>.<field>`.
    """
    if not resolved_params:
        raise ValueError("`resolved_params` is required to process parent data item")

    parent_resource_name = resolved_params[0].resolve_config["resource"]
    params_values: Dict[str, Any] = {}

    for resolved_param in resolved_params:
        field_values = jsonpath.find_values(resolved_param.field_path, item)
        if not field_values:
            field_path = resolved_param.resolve_config["field"]
            raise ValueError(
                f"Resource expects a field `{field_path}` to be present in the incoming data from"
                f" resource `{parent_resource_name}` in order to bind it to path param:"
                f" `{resolved_param.param_name}`. Available parent fields are:"
                f" {list(item.keys())}"
            )

        params_values[resolved_param.param_name] = field_values[0]

    if incremental:
        params_values["incremental"] = incremental
        if incremental_value_convert:
            params_values.update(convert_incremental_values(incremental, incremental_value_convert))

    return params_values


def _extract_single_placeholder_field(value: str) -> Tuple[bool, Optional[str]]:
    """Check if a string contains exactly one placeholder with no surrounding text
    if so, return True and the field name, otherwise return False and None

    Args:
        value: The string to check

    Returns:
        Tuple[bool, Optional[str]]: (True, field_name) if it's a single
            placeholder, (False, None) otherwise
    """
    parsed = list(string.Formatter().parse(value))
    if (
        len(parsed) == 1
        and parsed[0][0] == ""  # no leading text
        and (parsed[0][2] is None or parsed[0][2] == "")  # no format spec
        and parsed[0][3] is None  # no conversion
        and parsed[0][1]  # we have a placeholder
    ):
        return True, parsed[0][1]
    return False, None


def expand_placeholders(
    obj: Any, placeholders: Dict[str, Any], preserve_value_type: bool = False
) -> Any:
    """
    Recursively expand str.format placeholders in `obj` using `placeholders`.

    Args:
        obj: The object to expand placeholders in
        placeholders: Dictionary of placeholder values
        preserve_value_type: If True, when a string contains exactly one
            placeholder with no surrounding text, format spec, or conversion,
            return the value directly instead of formatting it as a string.
            This preserves the original type of the value. Defaults to False.
    """
    if obj is None:
        return None

    if isinstance(obj, str):
        if preserve_value_type:
            is_single, field_name = _extract_single_placeholder_field(obj)
            if is_single:
                value, _ = DirectKeyFormatter().get_field(field_name, (), placeholders)
                return value

        return DirectKeyFormatter().format(obj, **placeholders)

    if isinstance(obj, dict):
        return {
            expand_placeholders(k, placeholders, preserve_value_type): expand_placeholders(
                v, placeholders, preserve_value_type
            )
            for k, v in obj.items()
        }

    if isinstance(obj, list):
        return [expand_placeholders(item, placeholders, preserve_value_type) for item in obj]

    return obj  # For other data types, do nothing


def build_parent_record(
    item: Dict[str, Any],
    parent_resource_name: str,
    include_from_parent: Optional[List[str]],
) -> Dict[str, Any]:
    """
    Builds a dictionary of the `include_from_parent` fields from the parent,
    renaming them using `make_parent_key_name`.
    """
    parent_record: Dict[str, Any] = {}
    if not include_from_parent:
        return parent_record

    for parent_key in include_from_parent:
        child_key = make_parent_key_name(parent_resource_name, parent_key)
        if parent_key not in item:
            raise ValueError(
                f"Resource expects a field `{parent_key}` to be present in the incoming data "
                f"from resource `{parent_resource_name}` in order to include it in child records"
                f" under `{child_key}`. Available parent fields are: {list(item.keys())}"
            )
        parent_record[child_key] = item[parent_key]
    return parent_record


def _merge_resource_endpoints(
    default_config: EndpointResourceBase, config: EndpointResource
) -> EndpointResource:
    """Merges `default_config` and `config`, returns new instance of EndpointResource"""
    # NOTE: config is normalized and always has "endpoint" field which is a dict
    # TODO: could deep merge paginators and auths of the same type

    default_endpoint = default_config.get("endpoint", Endpoint())
    assert isinstance(default_endpoint, dict)
    config_endpoint = config["endpoint"]
    assert isinstance(config_endpoint, dict)

    merged_endpoint: Endpoint = {
        **default_endpoint,
        **{k: v for k, v in config_endpoint.items() if k not in ("json", "params")},  # type: ignore[typeddict-item]
    }
    # merge endpoint, only params and json are allowed to deep merge
    if "json" in config_endpoint:
        merged_endpoint["json"] = {
            **(merged_endpoint.get("json", {})),
            **config_endpoint["json"],
        }
    if "params" in config_endpoint:
        merged_endpoint["params"] = {
            **(merged_endpoint.get("params", {})),
            **config_endpoint["params"],
        }
    # merge columns
    if (default_columns := default_config.get("columns")) and (columns := config.get("columns")):
        # merge only native dlt formats, skip pydantic and others
        if isinstance(columns, (list, dict)) and isinstance(default_columns, (list, dict)):
            # normalize columns
            columns = ensure_table_schema_columns(columns)
            default_columns = ensure_table_schema_columns(default_columns)
            # merge columns with deep merging hints
            config["columns"] = merge_columns(copy(default_columns), columns, merge_columns=True)

    # no need to deep merge resources
    merged_resource: EndpointResource = {
        **default_config,
        **config,
        "endpoint": merged_endpoint,
    }
    return merged_resource


def _get_available_contexts(endpoint: Endpoint) -> Set[str]:
    """Returns a list of available contexts for the endpoint.
    Args:
        endpoint (Endpoint): The endpoint configuration to check

    Returns:
        List[str]: List of available context names
    """
    contexts = {"resources"}  # resources context is always available

    if "incremental" in endpoint:
        contexts.add("incremental")

    return contexts


def _raise_if_any_not_in(expressions: Set[str], available_contexts: Set[str], message: str) -> None:
    """Validates that all expressions start with one of the available contexts.

    Args:
        expressions: Set of expressions to validate
        available_contexts: Set of valid context prefixes (e.g. 'resources', 'incremental')
        message: Location where invalid expression was found (for error message)

    Raises:
        ValueError: If any expression doesn't start with an available context prefix
    """
    for expression in expressions:
        if not any(expression.startswith(prefix + ".") for prefix in available_contexts):
            raise ValueError(
                f"Expression `{expression}` defined in `{message}` is not valid. Valid expressions"
                f" must start with one of: `{available_contexts}`. If you need to use"
                " literal curly braces in your expression, escape them by doubling them: {{ and"
                " }}"
            )


def _set_incremental_params(
    params: Dict[str, Any],
    incremental_object: Incremental[Any],
    incremental_param: IncrementalParam,
    transform: Optional[Callable[..., Any]],
) -> Dict[str, Any]:
    def identity_func(x: Any) -> Any:
        return x

    if transform is None:
        transform = identity_func
    if incremental_param.start:
        params[incremental_param.start] = transform(incremental_object.last_value)
    if incremental_param.end:
        params[incremental_param.end] = transform(incremental_object.end_value)
    return params


def paginate_dependent_resource(
    items: List[Dict[str, Any]],
    method: HTTPMethodBasic,
    path: str,
    headers: Optional[Dict[str, Any]],
    params: Dict[str, Any],
    json: Optional[Dict[str, Any]],
    paginator: Optional[BasePaginator],
    data_selector: Optional[jsonpath.TJsonPath],
    hooks: Optional[Dict[str, Any]],
    client: RESTClient,
    resolved_params: List[ResolvedParam],
    include_from_parent: List[str],
    incremental_object: Optional[Incremental[Any]],
    incremental_param: Optional[IncrementalParam],
    incremental_cursor_transform: Optional[Callable[..., Any]],
) -> Generator[Any, None, None]:
    if incremental_object:
        params = _set_incremental_params(
            params,
            incremental_object,
            incremental_param,
            incremental_cursor_transform,
        )

    for item in items:
        processed_data = process_parent_data_item(
            path=path,
            item=item,
            headers=headers,
            params=params,
            request_json=json,
            resolved_params=resolved_params,
            include_from_parent=include_from_parent,
            incremental=incremental_object,
            incremental_value_convert=incremental_cursor_transform,
        )

        for child_page in client.paginate(
            method=method,
            path=processed_data.path,
            headers=processed_data.headers,
            params=processed_data.params,
            json=processed_data.json,
            paginator=paginator,
            data_selector=data_selector,
            hooks=hooks,
        ):
            if processed_data.parent_record:
                for child_record in child_page:
                    child_record.update(processed_data.parent_record)
            yield child_page


def paginate_resource(
    method: HTTPMethodBasic,
    path: str,
    headers: Optional[Dict[str, Any]],
    params: Dict[str, Any],
    json: Optional[Dict[str, Any]],
    paginator: Optional[BasePaginator],
    data_selector: Optional[jsonpath.TJsonPath],
    hooks: Optional[Dict[str, Any]],
    client: RESTClient,
    incremental_object: Optional[Incremental[Any]],
    incremental_param: Optional[IncrementalParam],
    incremental_cursor_transform: Optional[Callable[..., Any]],
) -> Generator[Any, None, None]:
    format_kwargs = {}
    if incremental_object:
        params = _set_incremental_params(
            params,
            incremental_object,
            incremental_param,
            incremental_cursor_transform,
        )
        format_kwargs["incremental"] = incremental_object
        if incremental_cursor_transform:
            format_kwargs.update(
                convert_incremental_values(incremental_object, incremental_cursor_transform)
            )

    # Always expand placeholders to handle escaped sequences
    path = expand_placeholders(path, format_kwargs)
    headers = expand_placeholders(headers, format_kwargs)
    params = expand_placeholders(params, format_kwargs)
    json = expand_placeholders(json, format_kwargs, preserve_value_type=True)

    yield from client.paginate(
        method=method,
        path=path,
        headers=headers,
        params=params,
        json=json,
        paginator=paginator,
        data_selector=data_selector,
        hooks=hooks,
    )
