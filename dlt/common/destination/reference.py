from abc import ABC, abstractmethod
from importlib import import_module

from typing import (
    Callable,
    ClassVar,
    List,
    Optional,
    Type,
    Union,
    Dict,
    Any,
    TypeVar,
    Generic,
)
from typing_extensions import TypeAlias
import inspect

from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.configuration.specs.pluggable_run_context import SupportsRunContext
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.configuration import resolve_configuration, known_sections
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    InvalidDestinationReference,
    UnknownDestinationModule,
)
from dlt.common.destination.client import DestinationClientConfiguration, JobClientBase
from dlt.common.runtime.run_context import RunContext
from dlt.common.schema.schema import Schema


TDestinationConfig = TypeVar("TDestinationConfig", bound="DestinationClientConfiguration")
TDestinationClient = TypeVar("TDestinationClient", bound="JobClientBase")
AnyDestination: TypeAlias = "Destination[DestinationClientConfiguration, JobClientBase]"
AnyDestination_CO: TypeAlias = "Destination[Any, Any]"


# TODO: type Destination properly
TDestinationReferenceArg = Union[str, AnyDestination_CO, Callable[..., AnyDestination_CO], None]


class Destination(ABC, Generic[TDestinationConfig, TDestinationClient]):
    """A destination factory that can be partially pre-configured
    with credentials and other config params.
    """

    DESTINATIONS: ClassVar[Dict[str, Type[AnyDestination]]] = {}
    """A registry of all the decorated destinations"""
    CONTEXT: ClassVar[SupportsRunContext] = None

    config_params: Dict[str, Any]
    """Explicit config params, overriding any injected or default values."""
    caps_params: Dict[str, Any]
    """Explicit capabilities params, overriding any default values for this destination"""

    def __init__(self, **kwargs: Any) -> None:
        # Create initial unresolved destination config
        # Argument defaults are filtered out here because we only want arguments passed explicitly
        # to supersede config from the environment or pipeline args
        # __orig_base__ tells where the __init__ of interest is, in case class is derived
        sig = inspect.signature(getattr(self.__class__, "__orig_base__", self.__class__).__init__)
        params = sig.parameters

        # get available args
        spec = self.spec
        spec_fields = spec.get_resolvable_fields()
        caps_fields = DestinationCapabilitiesContext.get_resolvable_fields()

        # remove default kwargs
        kwargs = {k: v for k, v in kwargs.items() if k not in params or v != params[k].default}

        # warn on unknown params
        for k in list(kwargs):
            if k not in spec_fields and k not in caps_fields:
                logger.warning(
                    f"When initializing destination factory of type {self.destination_type},"
                    f" argument {k} is not a valid field in {spec.__name__} or destination"
                    " capabilities"
                )
                kwargs.pop(k)

        self.config_params = {k: v for k, v in kwargs.items() if k in spec_fields}
        self.caps_params = {k: v for k, v in kwargs.items() if k in caps_fields}

    @property
    @abstractmethod
    def spec(self) -> Type[TDestinationConfig]:
        """A spec of destination configuration that also contains destination credentials"""
        ...

    def capabilities(
        self, config: Optional[TDestinationConfig] = None, naming: Optional[NamingConvention] = None
    ) -> DestinationCapabilitiesContext:
        """Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc.
        Explicit caps arguments passed to the factory init and stored in `caps_params` are applied.

        If `config` is provided, it is used to adjust the capabilities, otherwise the explicit config composed just of `config_params` passed
          to factory init is applied
        If `naming` is provided, the case sensitivity and case folding are adjusted.
        """
        caps = self._raw_capabilities()
        caps.update(self.caps_params)
        # get explicit config if final config not passed
        if config is None:
            # create mock credentials to avoid credentials being resolved
            init_config = self.spec()
            init_config.update(self.config_params)
            credentials = self.spec.credentials_type(init_config)()
            credentials.__is_resolved__ = True
            config = self.spec(credentials=credentials)
            try:
                config = self.configuration(config, accept_partial=True)
            except Exception:
                # in rare cases partial may fail ie. when invalid native value is present
                # in that case we fallback to "empty" config
                pass
        return self.adjust_capabilities(caps, config, naming)

    @abstractmethod
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        """Returns raw capabilities, before being adjusted with naming convention and config"""
        ...

    @property
    def destination_name(self) -> str:
        """The destination name will either be explicitly set while creating the destination or will be taken from the type"""
        return self.config_params.get("destination_name") or self.to_name(self.destination_type)

    @property
    def destination_type(self) -> str:
        full_path = self.__class__.__module__ + "." + self.__class__.__qualname__
        return Destination.normalize_type(full_path)

    @property
    def destination_description(self) -> str:
        return f"{self.destination_name}({self.destination_type})"

    @property
    @abstractmethod
    def client_class(self) -> Type[TDestinationClient]:
        """A job client class responsible for starting and resuming load jobs"""
        ...

    def configuration(
        self, initial_config: TDestinationConfig, accept_partial: bool = False
    ) -> TDestinationConfig:
        """Get a fully resolved destination config from the initial config"""

        config = resolve_configuration(
            initial_config or self.spec(),
            sections=(known_sections.DESTINATION, self.destination_name),
            # Already populated values will supersede resolved env config
            explicit_value=self.config_params,
            accept_partial=accept_partial,
        )
        return config

    def client(
        self, schema: Schema, initial_config: TDestinationConfig = None
    ) -> TDestinationClient:
        """Returns a configured instance of the destination's job client"""
        config = self.configuration(initial_config)
        return self.client_class(schema, config, self.capabilities(config, schema.naming))

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: TDestinationConfig,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        """Adjust the capabilities to match the case sensitivity as requested by naming convention."""
        # if naming not provided, skip the adjustment
        if not naming or not naming.is_case_sensitive:
            # all destinations are configured to be case insensitive so there's nothing to adjust
            return caps
        if not caps.has_case_sensitive_identifiers:
            if caps.casefold_identifier is str:
                logger.info(
                    f"Naming convention {naming.name()} is case sensitive but the destination does"
                    " not support case sensitive identifiers. Nevertheless identifier casing will"
                    " be preserved in the destination schema."
                )
            else:
                logger.warn(
                    f"Naming convention {naming.name()} is case sensitive but the destination does"
                    " not support case sensitive identifiers. Destination will case fold all the"
                    f" identifiers with {caps.casefold_identifier}"
                )
        else:
            # adjust case folding to store casefold identifiers in the schema
            if caps.casefold_identifier is not str:
                caps.casefold_identifier = str
                logger.info(
                    f"Enabling case sensitive identifiers for naming convention {naming.name()}"
                )
        return caps

    @staticmethod
    def to_name(ref: TDestinationReferenceArg) -> str:
        if ref is None:
            raise InvalidDestinationReference([])
        if isinstance(ref, str):
            return ref.rsplit(".", 1)[-1]
        if callable(ref):
            ref = ref()
        return ref.destination_name

    @staticmethod
    def normalize_type(destination_type: str) -> str:
        """Normalizes destination type string into a canonical form. Assumes that type names without dots correspond to built in destinations."""
        if "." not in destination_type:
            destination_type = "dlt.destinations." + destination_type
        # the next two lines shorten the dlt internal destination paths to dlt.destinations.<destination_type>
        name = Destination.to_name(destination_type)
        destination_type = destination_type.replace(
            f".destinations.impl.{name}.factory.", ".destinations."
        )
        return destination_type

    @classmethod
    def register(cls, destination_name: str) -> None:
        """Registers this factory class under `destination_name`. Optionally uses other context."""
        cls.CONTEXT = Container()[PluggableRunContext].context
        ref = f"{cls.CONTEXT.name}.{destination_name}"
        if ref in cls.DESTINATIONS:
            logger.info(
                f"A destination with ref {ref} is already registered and will be overwritten"
            )
        cls.DESTINATIONS[ref] = cls  # type: ignore[assignment]

    @staticmethod
    def to_fully_qualified_refs(ref: str) -> List[str]:
        """Converts ref into fully qualified form, return one or more alternatives for shorthand notations.
        Run context is injected if needed. Following formats are recognized
        - context_name.'destinations'.name (fully qualified)
        - 'destinations'.name
        - name
        NOTE: the last component of destination type serves as destination name if not explicitly specified
        """
        ref_split = ref.split(".")
        ref_parts = len(ref_split)
        if ref_parts < 2 or (ref_parts == 2 and ref_split[1] == known_sections.DESTINATIONS):
            # context name is needed
            refs = []
            run_names = [Container()[PluggableRunContext].context.name]
            # always look in default run context
            if run_names[0] != RunContext.CONTEXT_NAME:
                run_names.append(RunContext.CONTEXT_NAME)
            for run_name in run_names:
                if ref_parts == 1:
                    # ref is: name
                    refs.append(f"{run_name}.{known_sections.DESTINATIONS}.{ref}")
                else:
                    # ref is: `destinations`.name`
                    refs.append(f"{run_name}.{ref}")
            return refs
        if len(ref_split) == 3 and ref_split[1] == known_sections.DESTINATIONS:
            return [ref]

        return []

    @classmethod
    def from_reference(
        cls,
        ref: TDestinationReferenceArg,
        credentials: Optional[Any] = None,
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[AnyDestination]:
        """Instantiate destination from str reference.
        The ref can be a destination name or import path pointing to a destination class (e.g. `dlt.destinations.postgres`)
        """
        # if we only get a name but no ref, we assume that the name is the destination_type
        if ref is None and destination_name is not None:
            ref = destination_name
        if ref is None:
            return None
        # evaluate callable returning Destination
        if callable(ref):
            ref = ref()
        if isinstance(ref, Destination):
            if credentials or destination_name or environment:
                logger.warning(
                    "Cannot override credentials, destination_name or environment when passing a"
                    " Destination instance, these values will be ignored."
                )
            return ref

        if not isinstance(ref, str):
            raise InvalidDestinationReference(ref)

        # resolve ref
        refs = cls.to_fully_qualified_refs(ref)
        factory: Type[AnyDestination] = None

        for ref_ in refs:
            if factory := cls.DESTINATIONS.get(ref_):
                break

        # no reference found, try to import default module
        if not factory:
            # try ref, normalized refs and ref without the context name
            refs.extend(set([r.split(".", 1)[1] for r in refs]))
            if "." in ref and ref not in refs:
                refs = [ref] + refs
            for possible_type in refs:
                try:
                    module_path, attr_name = possible_type.rsplit(".", 1)
                    dest_module = import_module(module_path)
                except ModuleNotFoundError:
                    continue

                try:
                    factory = getattr(dest_module, attr_name)
                except AttributeError as e:
                    raise UnknownDestinationModule(ref) from e
                break
        if not factory:
            raise UnknownDestinationModule(ref)

        if credentials:
            kwargs["credentials"] = credentials
        if destination_name:
            kwargs["destination_name"] = destination_name
        if environment:
            kwargs["environment"] = environment
        try:
            dest = factory(**kwargs)
            dest.spec
        except Exception as e:
            raise InvalidDestinationReference(ref) from e
        return dest
