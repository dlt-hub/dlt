import os
import sys
import logging
import time
from collections import defaultdict
from typing import (
    Any,
    ContextManager,
    Dict,
    TYPE_CHECKING,
    DefaultDict,
    NamedTuple,
    Optional,
    Union,
    TextIO,
)

if TYPE_CHECKING:
    from tqdm import tqdm
    import enlighten
    from enlighten import Counter as EnlCounter, StatusBar as EnlStatusBar, Manager as EnlManager
    from alive_progress import alive_bar
    # OpenTelemetry types for type checking only
    from opentelemetry import trace as otel_trace, metrics as otel_metrics
    from opentelemetry.trace import Span, SpanKind, Status, StatusCode
    from opentelemetry.metrics import Counter as OtelCounter, Histogram
    # Pipeline types for type checking only
    from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace
    from dlt.pipeline.typing import TPipelineStep
    from dlt.common.pipeline import SupportsPipeline
else:
    tqdm = EnlCounter = EnlStatusBar = EnlManager = Any
    # OpenTelemetry types set to Any when not type checking
    otel_trace = otel_metrics = Span = SpanKind = Status = StatusCode = Any
    OtelCounter = Histogram = Any
    # Pipeline types set to Any when not type checking
    PipelineTrace = PipelineStepTrace = TPipelineStep = SupportsPipeline = Any

from dlt.common import logger as dlt_logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.runtime.collector_base import Collector, TCollector

class NullCollector(Collector):
    """A default counter that does not count anything."""

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        pass

    def _start(self, step: str) -> None:
        pass

    def _stop(self) -> None:
        pass


class DictCollector(Collector):
    """A collector that just counts"""

    def __init__(self) -> None:
        self.step = None
        self.counters: DefaultDict[str, int] = None

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        assert not label, "labels not supported in dict collector"
        self.counters[name] += inc

    def _start(self, step: str) -> None:
        self.counters = defaultdict(int)

    def _stop(self) -> None:
        self.counters = None


class LogCollector(Collector):
    """A Collector that shows progress by writing to a Python logger or a console"""

    logger: Union[logging.Logger, TextIO]
    log_level: int

    class CounterInfo(NamedTuple):
        description: str
        start_time: float
        total: Optional[int]

    def __init__(
        self,
        log_period: float = 1.0,
        logger: Union[logging.Logger, TextIO] = sys.stdout,
        log_level: int = logging.INFO,
        dump_system_stats: bool = True,
    ) -> None:
        """
        Collector writing to a `logger` every `log_period` seconds. The logger can be a Python logger instance, text stream, or None that will attach `dlt` logger

        Args:
            log_period (float, optional): Time period in seconds between log updates. Defaults to 1.0.
            logger (logging.Logger | TextIO, optional): Logger or text stream to write log messages to. Defaults to stdio.
            log_level (str, optional): Log level for the logger. Defaults to INFO level
            dump_system_stats (bool, optional): Log memory and cpu usage. Defaults to True
        """
        self.step = None
        self.log_period = log_period
        self.logger = logger
        self.log_level = log_level
        self.counters: DefaultDict[str, int] = None
        self.counter_info: Dict[str, LogCollector.CounterInfo] = None
        self.messages: Dict[str, Optional[str]] = None
        if dump_system_stats:
            try:
                import psutil
            except ImportError:
                self._log(
                    logging.WARNING,
                    "psutil dependency is not installed and mem stats will not be available. add"
                    " psutil to your environment or pass dump_system_stats argument as False to"
                    " disable warning.",
                )
                dump_system_stats = False
        self.dump_system_stats = dump_system_stats
        self.last_log_time: float = None

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        counter_key = f"{name}_{label}" if label else name

        if counter_key not in self.counters:
            self.counters[counter_key] = 0
            self.counter_info[counter_key] = LogCollector.CounterInfo(
                description=f"{name} ({label})" if label else name,
                start_time=time.time(),
                total=total,
            )
            self.messages[counter_key] = None
            self.last_log_time = None
        else:
            counter_info = self.counter_info[counter_key]
            if inc_total:
                self.counter_info[counter_key] = LogCollector.CounterInfo(
                    description=counter_info.description,
                    start_time=counter_info.start_time,
                    total=counter_info.total + inc_total,
                )

        self.counters[counter_key] += inc
        if message is not None:
            self.messages[counter_key] = message
        self.maybe_log()

    def maybe_log(self) -> None:
        """Check if should report and if so, call self.on_log"""
        current_time = time.time()
        if self.last_log_time is None or current_time - self.last_log_time >= self.log_period:
            self.on_log()
            self.last_log_time = time.time()

    def _counter_to_log_line(
        self, counter_key: str, count: int, info: CounterInfo, current_time: float
    ) -> str:
        """
        Convert a single counter to a log line.
        Example:
            Resources: 0/1 (0.0%) | Time: 0.01s | Rate: 0.00/s
        """
        elapsed_time = current_time - info.start_time
        items_per_second = (count / elapsed_time) if elapsed_time > 0 else 0

        progress = f"{count}/{info.total}" if info.total else f"{count}"
        percentage = f"({count / info.total * 100:.1f}%)" if info.total else ""
        elapsed_time_str = f"{elapsed_time:.2f}s"
        items_per_second_str = f"{items_per_second:.2f}/s"
        message = (
            f"[{self.messages[counter_key]}]" if self.messages[counter_key] is not None else ""
        )

        return (
            f"{info.description}: {progress} {percentage} | Time: {elapsed_time_str} | Rate:"
            f" {items_per_second_str} {message}"
        ).strip()

    def _system_stats_to_log_line(self) -> str:
        """Convert system stats to a log line format."""
        try:
            import psutil

            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            current_mem = mem_info.rss / (1024**2)  # Convert to MB
            mem_percent = psutil.virtual_memory().percent
            cpu_percent = process.cpu_percent()
            return (
                f"Memory usage: {current_mem:.2f} MB ({mem_percent:.2f}%) | CPU usage:"
                f" {cpu_percent:.2f}%"
            )
        except ImportError:
            return "System stats unavailable (psutil not installed)"

    def dump_counters(self) -> None:
        """Dump all counters to log using the shared formatting methods."""
        current_time = time.time()
        log_lines = []

        step_header = f" {self.step} ".center(80, "-")
        log_lines.append(step_header)

        for name, count in self.counters.items():
            info = self.counter_info[name]
            log_lines.append(self._counter_to_log_line(name, count, info, current_time))

        if self.dump_system_stats:
            log_lines.append(self._system_stats_to_log_line())

        log_lines.append("")
        log_message = "\n".join(log_lines)
        if not self.logger:
            # try to attach dlt logger
            self.logger = dlt_logger.LOGGER
        self._log(self.log_level, log_message)

    def _log(self, log_level: int, log_message: str) -> None:
        if isinstance(self.logger, (logging.Logger, logging.LoggerAdapter)):
            self.logger.log(log_level, log_message)
        else:
            print(log_message, file=self.logger or sys.stdout)  # noqa

    def _start(self, step: str) -> None:
        self.counters = defaultdict(int)
        self.counter_info = {}
        self.messages = {}
        self.last_log_time = time.time()

    def _stop(self) -> None:
        self.on_log()
        self.counters = None
        self.counter_info = None
        self.messages = None
        self.last_log_time = None

    def on_log(self) -> None:
        self.dump_counters()


class TqdmCollector(Collector):
    """A Collector that shows progress with `tqdm` progress bars"""

    def __init__(self, single_bar: bool = False, **tqdm_kwargs: Any) -> None:
        """A Collector that uses tqdm to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to tqdm in kwargs"""
        try:
            global tqdm
            from tqdm import tqdm
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "TqdmCollector", ["tqdm"], "We need tqdm to display progress bars."
            )
        self.step = None
        self.single_bar = single_bar
        self._bars: Dict[str, tqdm[None]] = {}
        self.tqdm_kwargs = tqdm_kwargs or {}

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = "",
    ) -> None:
        key = f"{name}_{label}"
        bar = self._bars.get(key)
        if bar is None:
            if label:
                name = f"{name}[{label}]"
            if len(self._bars) == 0:
                desc = self.step + ": " + name
            else:
                # do not add any more counters
                if self.single_bar:
                    return
                desc = name
            bar = tqdm(desc=desc, total=total, leave=False, **self.tqdm_kwargs)
            bar.refresh()
            self._bars[key] = bar
        else:
            if inc_total:
                bar.total += inc_total
                bar.refresh()
        if message:
            bar.set_postfix_str(message)
        bar.update(inc)

    def _start(self, step: str) -> None:
        self._bars = {}

    def _stop(self) -> None:
        for bar in self._bars.values():
            bar.refresh()
            bar.close()
        self._bars.clear()


class AliveCollector(Collector):
    """A Collector that shows progress with `alive-progress` progress bars"""

    def __init__(self, single_bar: bool = True, **alive_kwargs: Any) -> None:
        """Collector that uses alive_progress to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to alive_progress in kwargs"""
        try:
            global alive_bar
            from alive_progress import alive_bar

        except ModuleNotFoundError:
            raise MissingDependencyException(
                "AliveCollector",
                ["alive-progress"],
                "We need alive-progress to display progress bars.",
            )
        self.step = None
        self.single_bar = single_bar
        self._bars: Dict[str, Any] = {}
        self._bars_counts: Dict[str, int] = {}
        self._bars_contexts: Dict[str, ContextManager[Any]] = {}
        self.alive_kwargs = alive_kwargs or {}

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = "",
    ) -> None:
        key = f"{name}_{label}"
        bar = self._bars.get(key)
        if bar is None:
            if label:
                name = f"{name}[{label}]"
            if len(self._bars) == 0:
                desc = self.step + ": " + name
            else:
                # do not add any more counters
                if self.single_bar:
                    return
                desc = name
            bar = alive_bar(total=total, title=desc, **self.alive_kwargs)
            self._bars_contexts[key] = bar
            bar = self._bars[key] = bar.__enter__()
            self._bars_counts[key] = 0
        else:
            # TODO: implement once total change is supported
            pass

        # if message:
        #     bar.set_postfix_str(message)
        if inc > 0:
            bar(inc)
            self._bars_counts[key] += inc

    def _start(self, step: str) -> None:
        self._bars = {}
        self._bars_contexts = {}
        self

    def _stop(self) -> None:
        for bar in self._bars_contexts.values():
            bar.__exit__(None, None, None)
        self._bars.clear()
        self._bars_contexts.clear()
        self._bars_counts.clear()


class EnlightenCollector(Collector):
    """A Collector that shows progress with `enlighten` progress and status bars that also allow for logging."""

    _bars: Dict[str, EnlCounter]
    _manager: EnlManager
    _status: EnlStatusBar

    def __init__(self, single_bar: bool = False, **enlighten_kwargs: Any) -> None:
        """Collector that uses Enlighten to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to Enlighten in kwargs"""
        try:
            global enlighten

            import enlighten
            from enlighten import (
                Counter as EnlCounter,
                StatusBar as EnlStatusBar,
                Manager as EnlManager,
            )
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "EnlightenCollector",
                ["enlighten"],
                "We need enlighten to display progress bars with a space for log messages.",
            )
        self.step = None
        self.single_bar = single_bar
        self.enlighten_kwargs = enlighten_kwargs

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = "",
    ) -> None:
        key = f"{name}_{label}"
        bar = self._bars.get(key)
        if bar is None:
            if label:
                name = f"{name}[{label}]"
            if len(self._bars) > 0 and self.single_bar:
                # do not add any more counters
                return
            bar = self._manager.counter(
                desc=name, total=total, leave=True, force=True, **self.enlighten_kwargs
            )
            bar.refresh()
            self._bars[key] = bar
        else:
            if inc_total:
                bar.total = bar.total + inc_total
        bar.update(inc)

    def _start(self, step: str) -> None:
        self._bars = {}
        self._manager = enlighten.get_manager(enabled=True)
        self._status = self._manager.status_bar(
            leave=True, justify=enlighten.Justify.CENTER, fill="="
        )
        self._status.update(step)

    def _stop(self) -> None:
        if self._status:
            self._status.close()
        for bar in self._bars.values():
            bar.refresh()
            bar.close()
        self._bars.clear()
        self._manager.stop()
        self._manager = None
        self._bars = None
        self._status = None


class OpenTelemetryCollector(LogCollector):
    """
    OpenTelemetry collector that hooks into DLT's native trace callbacks.

    This collector extends LogCollector to leverage DLT's built-in counter tracking
    and logging while adding comprehensive OpenTelemetry metrics and tracing support.

    Advantages:
    - Leverages DLT's LogCollector for counter tracking and logging
    - Adds structured OpenTelemetry metrics (rows, duration, tables, runs)
    - Provides detailed step-specific metrics extraction
    - Supports optional logging (can be disabled for pure OTEL scenarios)
    - Uses DLT's official Collector API
    """

    __version__ = "0.1.0"

    def __init__(
        self,
        # LogCollector parameters
        log_period: float = 1.0,
        logger: Union[logging.Logger, TextIO, None] = sys.stdout,
        log_level: int = logging.INFO,
        dump_system_stats: bool = True,
        enable_logging: bool = True,
        # OpenTelemetry parameters
        tracer: Optional["otel_trace.Tracer"] = None,
        meter: Optional["otel_metrics.Meter"] = None,
        capture_per_table_metrics: bool = True,
    ):
        """
        Initialize OpenTelemetry collector.

        Args:
            log_period: Time period in seconds between log updates (default: 1.0)
            logger: Logger or text stream to write log messages to (default: stdout)
            log_level: Log level for the logger (default: INFO)
            dump_system_stats: Log memory and CPU usage (default: True)
            enable_logging: Whether to enable DLT's logging functionality (default: True)
            tracer: OpenTelemetry tracer (if None, will use global tracer)
            meter: OpenTelemetry meter (if None, will use global meter)
            capture_per_table_metrics: Whether to capture per-table row counts as span attributes
        """
        # Lazy import OpenTelemetry dependencies
        try:
            from opentelemetry import trace as otel_trace, metrics as otel_metrics, context
            from opentelemetry.trace import Span, SpanKind, Status, StatusCode
            from opentelemetry.metrics import Counter, Histogram
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "OpenTelemetryCollector",
                ["opentelemetry-api", "opentelemetry-sdk"],
                "We need OpenTelemetry to export traces and metrics.",
            )

        # Store references to the imported modules/classes for use in instance methods
        self._otel_trace = otel_trace
        self._otel_metrics = otel_metrics
        self._otel_context = context
        self._SpanKind = SpanKind
        self._Status = Status
        self._StatusCode = StatusCode

        # Initialize LogCollector if logging is enabled
        if enable_logging:
            super().__init__(log_period, logger, log_level, dump_system_stats)
        else:
            # Initialize minimal state without logging
            # Note: counters, counter_info, messages, and last_log_time are initialized
            # by _start() when the collector is used as a context manager
            self.step = None
            self.log_period = log_period
            self.logger = logger
            self.log_level = log_level
            self.dump_system_stats = dump_system_stats

        self.enable_logging = enable_logging
        self.capture_per_table_metrics = capture_per_table_metrics

        # Get or create tracer
        if tracer is None:
            tracer = otel_trace.get_tracer(
                "opentelemetry.instrumentation.dlt",
                instrumenting_library_version=self.__version__,
            )
        self.tracer = tracer

        # Get or create meter
        if meter is None:
            meter = otel_metrics.get_meter(
                "opentelemetry.instrumentation.dlt",
                version=self.__version__,
            )
        self.meter = meter

        # Create metrics instruments
        self._setup_metrics()

        # Store active spans for each step
        self._step_spans: Dict[str, Span] = {}
        self._step_tokens: Dict[str, Any] = {}  # Store context tokens for detaching
        self._root_span: Optional[Span] = None
        self._root_context: Optional[Any] = None
        self._root_token: Optional[Any] = None  # Store root context token

        # Store pipeline attributes for metrics context
        self._pipeline_attributes: Dict[str, str] = {}

    def _setup_metrics(self) -> None:
        """Create OpenTelemetry metrics instruments"""
        self.rows_counter: "OtelCounter" = self.meter.create_counter(
            name="dlt.pipeline.rows.total",
            description="Total number of rows processed",
            unit="1",
        )

        self.duration_histogram: "Histogram" = self.meter.create_histogram(
            name="dlt.pipeline.step.duration",
            description="Duration of pipeline steps",
            unit="s",
        )

        self.tables_counter: "OtelCounter" = self.meter.create_counter(
            name="dlt.pipeline.tables.count",
            description="Number of tables in the pipeline",
            unit="1",
        )

        self.pipeline_runs_counter: "OtelCounter" = self.meter.create_counter(
            name="dlt.pipeline.runs.total",
            description="Total number of pipeline runs",
            unit="1",
        )

    def update(
        self,
        name: str,
        inc: int = 1,
        total: Optional[int] = None,
        inc_total: Optional[int] = None,
        message: Optional[str] = None,
        label: Optional[str] = None,
    ) -> None:
        """
        Update counter for DLT's internal tracking and logging.

        This method leverages DLT's LogCollector.update() for counter tracking
        and logging. Dynamic per-counter metrics are not sent to OpenTelemetry
        to avoid metric cardinality explosion. Instead, aggregate metrics are
        captured in the step handlers (rows, tables, duration, etc.).
        """
        # Call parent to leverage DLT's built-in counter tracking and logging
        if self.enable_logging:
            super().update(name, inc, total, inc_total, message, label)
        else:
            # Minimal counter tracking without logging
            counter_key = f"{name}_{label}" if label else name
            if counter_key not in self.counters:
                self.counters[counter_key] = 0
            self.counters[counter_key] += inc

    def on_log(self) -> None:
        """
        Override on_log to add system metrics to OpenTelemetry.

        This is called periodically by LogCollector to log counters.
        We add system metrics to OpenTelemetry here.
        """
        # Call parent for logging if enabled
        if self.enable_logging:
            super().on_log()

        # Send system metrics to OpenTelemetry if enabled
        if self.dump_system_stats and self.meter:
            self._send_system_metrics()

    def _send_system_metrics(self) -> None:
        """Send system metrics (CPU, memory) to OpenTelemetry"""
        try:
            import psutil
            import os as os_module

            process = psutil.Process(os_module.getpid())
            mem_info = process.memory_info()

            # Get current system stats
            current_mem_mb = mem_info.rss / (1024**2)  # Convert to MB
            mem_percent = psutil.virtual_memory().percent
            cpu_percent = process.cpu_percent()

            # Create histogram instruments for system metrics (better for gauge-like values)
            if not hasattr(self, "_system_metric_instruments"):
                self._system_metric_instruments = {}

            if "memory_usage_mb" not in self._system_metric_instruments:
                self._system_metric_instruments["memory_usage_mb"] = (
                    self.meter.create_histogram(
                        name="dlt.system.memory_usage_mb",
                        description="Process memory usage in MB",
                        unit="MB",
                    )
                )

            if "memory_usage_percent" not in self._system_metric_instruments:
                self._system_metric_instruments["memory_usage_percent"] = (
                    self.meter.create_histogram(
                        name="dlt.system.memory_percent",
                        description="System memory usage percentage",
                        unit="percent",
                    )
                )

            if "cpu_usage_percent" not in self._system_metric_instruments:
                self._system_metric_instruments["cpu_usage_percent"] = (
                    self.meter.create_histogram(
                        name="dlt.system.cpu_percent",
                        description="Process CPU usage percentage",
                        unit="percent",
                    )
                )

            # Record current values with pipeline context (if available)
            attributes = {}
            if hasattr(self, "_pipeline_attributes"):
                attributes.update(self._pipeline_attributes)

            self._system_metric_instruments["memory_usage_mb"].record(
                current_mem_mb, attributes
            )
            self._system_metric_instruments["memory_usage_percent"].record(
                mem_percent, attributes
            )
            self._system_metric_instruments["cpu_usage_percent"].record(
                cpu_percent, attributes
            )

        except ImportError:
            pass  # psutil not available
        except Exception as e:
            dlt_logger.debug(f"Failed to send system metrics to OpenTelemetry: {e}")

    def _stop(self) -> None:
        """Stop collecting and flush metrics.

        Cleans up all resources including spans, counters, and metrics instruments.
        Should be called when the collector is no longer needed.

        NOTE: This is called by DLT's LogCollector after each step completes,
        so we should NOT clear the root span/context or step spans here.
        The root span/context will be cleared in on_end_trace().
        Step spans will be cleared in on_end_trace_step() when they're actually ended.
        """
        # DO NOT clear root span/context or step spans here
        # - Root span/context: needed for subsequent steps, cleared in on_end_trace()
        # - Step spans: needed for on_end_trace_step(), cleared when ended in on_end_trace_step()

        # Clear system metric instruments
        if hasattr(self, "_system_metric_instruments"):
            self._system_metric_instruments.clear()

        # Call parent to maintain logging behavior
        if self.enable_logging:
            super()._stop()
        else:
            # Clean up minimal state
            self.counters = None
            self.counter_info = None
            self.messages = None
            self.last_log_time = None

    def on_start_trace(
        self, trace: "PipelineTrace", step: "TPipelineStep", pipeline: "SupportsPipeline"
    ) -> None:
        """
        Called when pipeline trace starts.

        Creates the root span for the entire pipeline run.
        The step parameter is typically "run" for the full pipeline execution.
        """
        try:
            # Cache pipeline attributes for use in metrics
            self._pipeline_attributes = {
                "pipeline.name": pipeline.pipeline_name,
            }

            destination_name = "unknown"
            if hasattr(pipeline, "destination") and pipeline.destination:
                destination_name = getattr(
                    pipeline.destination, "destination_name", "unknown"
                )
            self._pipeline_attributes["pipeline.destination"] = destination_name
            self._pipeline_attributes["pipeline.dataset"] = (
                pipeline.dataset_name or "unknown"
            )

            # Use the step name from DLT (typically "run") as the root span name
            # This creates "dlt.run" as the root, with extract/normalize/load as children
            step_name = step if isinstance(step, str) else str(step)
            root_span_name = f"dlt.{step_name}"

            # Start root span for the entire pipeline run
            self._root_span = self.tracer.start_span(
                root_span_name,
                kind=self._SpanKind.INTERNAL,
            )

            # Set span in context for child spans
            self._root_context = self._otel_trace.set_span_in_context(self._root_span)

            # Attach the root context to make it active for auto-instrumentation
            # This allows libraries like requests to create child spans automatically
            self._root_token = self._otel_context.attach(self._root_context)

            # Set pipeline-level attributes
            self._root_span.set_attribute("pipeline.name", pipeline.pipeline_name)
            self._root_span.set_attribute("pipeline.destination", destination_name)
            self._root_span.set_attribute(
                "pipeline.dataset", pipeline.dataset_name or "unknown"
            )

            dlt_logger.debug(
                f"Started pipeline trace: {pipeline.pipeline_name} (root span: {root_span_name})"
            )

        except Exception as e:
            dlt_logger.warning(f"Failed to start pipeline trace: {e}", exc_info=True)

    def on_start_trace_step(
        self, trace: "PipelineTrace", step: "TPipelineStep", pipeline: "SupportsPipeline"
    ) -> None:
        """
        Called when a pipeline step starts.

        Creates a child span for the step.
        Note: This is NOT called for the root "run" step - only for extract/normalize/load.
        """
        try:
            # Extract step name - handle both string and object types
            if isinstance(step, str):
                step_name = step
            elif hasattr(step, "step"):
                step_name = step.step
            else:
                step_name = str(step)

            # Skip if this is the "run" step - it's already handled by on_start_trace
            if step_name == "run":
                dlt_logger.debug(
                    "Skipping duplicate 'run' step span - already created in on_start_trace"
                )
                return

            # Create a span for this step as child of root span
            step_span = self.tracer.start_span(
                f"dlt.{step_name}",
                kind=self._SpanKind.INTERNAL,
                context=self._root_context,
            )

            # Set this span as active in the context for auto-instrumentation
            # This allows HTTP libraries and other instrumented code to create child spans
            step_context = self._otel_trace.set_span_in_context(step_span, self._root_context)
            step_token = self._otel_context.attach(step_context)

            # Store span and token so we can end/detach them later
            self._step_spans[step_name] = step_span
            self._step_tokens[step_name] = step_token

        except Exception as e:
            dlt_logger.warning(f"Failed to start trace step: {e}", exc_info=True)

    def on_end_trace_step(
        self,
        trace: "PipelineTrace",
        step: "PipelineStepTrace",
        pipeline: "SupportsPipeline",
        step_info: Any,
        send_state: bool,
    ) -> None:
        """
        Called when a pipeline step completes.

        This is the main method where we extract metrics and complete spans.
        """
        step_name = step.step  # "extract", "normalize", or "load"

        # Skip "run" step - it's handled by on_end_trace, not on_end_trace_step
        if step_name == "run":
            dlt_logger.debug(
                "Skipping 'run' step in on_end_trace_step - it's handled by on_end_trace"
            )
            return

        span = self._step_spans.get(step_name)

        if not span:
            raise RuntimeError(
                f"No span found for step '{step_name}'. "
                f"on_start_trace_step was not called before on_end_trace_step. "
                f"This indicates a bug in DLT's trace lifecycle or the collector."
            )

        try:
            # Calculate duration
            duration = (step.finished_at - step.started_at).total_seconds()

            # Set timing attributes
            span.set_attribute(f"{step_name}.started_at", step.started_at.isoformat())
            span.set_attribute(f"{step_name}.finished_at", step.finished_at.isoformat())
            span.set_attribute(f"{step_name}.duration", duration)

            # Record duration metric
            self.duration_histogram.record(
                duration,
                attributes={
                    "pipeline.name": pipeline.pipeline_name,
                    "step": step_name,
                },
            )

            # Extract step-specific metrics
            if step_name == "normalize":
                self._handle_normalize_step(span, step_info, pipeline)
            elif step_name == "extract":
                self._handle_extract_step(span, step_info, pipeline)
            elif step_name == "load":
                self._handle_load_step(span, step_info, pipeline, trace)

            # Handle exceptions
            if step.step_exception:
                span.record_exception(step.step_exception)
                span.set_status(self._Status(self._StatusCode.ERROR, str(step.step_exception)))
                dlt_logger.error(f"Step {step_name} failed: {step.step_exception}")
            else:
                span.set_status(self._Status(self._StatusCode.OK))
                dlt_logger.debug(f"Step {step_name} completed successfully")

        except Exception as e:
            dlt_logger.warning(f"Error processing step {step_name}: {e}", exc_info=True)
            span.record_exception(e)
            span.set_status(self._Status(self._StatusCode.ERROR, str(e)))

        finally:
            # Detach the context before ending the span
            # This restores the previous context (root span context)
            if step_name in self._step_tokens:
                self._otel_context.detach(self._step_tokens[step_name])
                del self._step_tokens[step_name]

            # End the step span
            span.end()
            if step_name in self._step_spans:
                del self._step_spans[step_name]

    def _handle_normalize_step(
        self, span: "Span", normalize_info: Any, pipeline: "SupportsPipeline"
    ) -> None:
        """
        Extract comprehensive metrics from NormalizeInfo.

        Similar to LogCollector, this method accesses all available metrics
        from NormalizeInfo, primarily using the reliable row_counts property.
        """
        if not normalize_info:
            return

        total_rows = 0
        table_names = []

        # Method 1: Use the reliable row_counts property (primary method)
        if hasattr(normalize_info, "row_counts"):
            row_counts = normalize_info.row_counts
            if isinstance(row_counts, dict):
                total_rows = sum(row_counts.values())
                table_names = list(row_counts.keys())

                # Set span attributes
                span.set_attribute("normalize.rows", total_rows)
                span.set_attribute("normalize.tables", ",".join(table_names))
                span.set_attribute("normalize.table_count", len(table_names))

                # Per-table metrics (optional, can be verbose)
                if self.capture_per_table_metrics:
                    for table_name, count in row_counts.items():
                        span.set_attribute(f"normalize.rows.{table_name}", count)

        # Record metrics
        if total_rows > 0:
            self.rows_counter.add(
                total_rows,
                attributes={
                    "pipeline.name": pipeline.pipeline_name,
                    "step": "normalize",
                },
            )

        dlt_logger.debug(f"Normalize: {total_rows} rows across {len(table_names)} tables")

        # Load IDs
        if hasattr(normalize_info, "loads_ids") and normalize_info.loads_ids:
            loads_ids = normalize_info.loads_ids
            if isinstance(loads_ids, list) and loads_ids:
                span.set_attribute("normalize.load_id", loads_ids[0])
            elif loads_ids:
                span.set_attribute("normalize.load_id", str(loads_ids))

    def _handle_extract_step(
        self, span: "Span", extract_info: Any, pipeline: "SupportsPipeline"
    ) -> None:
        """
        Extract comprehensive metrics from ExtractInfo.

        Similar to LogCollector, this method accesses all available metrics
        from the ExtractInfo object's metrics structure.
        """
        if not extract_info:
            return

        total_rows = 0
        resource_names = set()
        table_names = set()
        schema_name = None

        # Method 1: Check for row_counts property (if available in some DLT versions)
        if hasattr(extract_info, "row_counts"):
            row_counts = extract_info.row_counts
            if isinstance(row_counts, dict):
                total_rows = sum(row_counts.values())
                table_names.update(row_counts.keys())

        # Method 2: Navigate through metrics structure (only if Method 1 didn't find rows)
        if total_rows == 0 and hasattr(extract_info, "metrics") and isinstance(extract_info.metrics, dict):
            for load_id, metrics_list in extract_info.metrics.items():
                if not isinstance(metrics_list, list):
                    continue

                for metrics in metrics_list:
                    # Handle both dict and object access patterns
                    # Get schema name
                    if not schema_name:
                        if isinstance(metrics, dict) and "schema_name" in metrics:
                            schema_name = metrics["schema_name"]
                        elif hasattr(metrics, "schema_name"):
                            schema_name = getattr(metrics, "schema_name", None)

                    # Get resource names and metrics
                    resource_metrics = None
                    if isinstance(metrics, dict) and "resource_metrics" in metrics:
                        resource_metrics = metrics["resource_metrics"]
                    elif hasattr(metrics, "resource_metrics"):
                        resource_metrics = getattr(metrics, "resource_metrics", None)

                    if resource_metrics:
                        if isinstance(resource_metrics, dict):
                            resource_names.update(resource_metrics.keys())
                        elif hasattr(resource_metrics, "keys"):
                            resource_names.update(resource_metrics.keys())

                    # Count rows from table metrics (comprehensive extraction)
                    table_metrics = None
                    if isinstance(metrics, dict) and "table_metrics" in metrics:
                        table_metrics = metrics["table_metrics"]
                    elif hasattr(metrics, "table_metrics"):
                        table_metrics = getattr(metrics, "table_metrics", None)

                    if table_metrics:
                        if isinstance(table_metrics, dict):
                            for table_name, table_metric in table_metrics.items():
                                table_names.add(table_name)
                                # Handle both dict and object access
                                if isinstance(table_metric, dict):
                                    total_rows += table_metric.get("items_count", 0)
                                elif hasattr(table_metric, "items_count"):
                                    total_rows += getattr(
                                        table_metric, "items_count", 0
                                    )
                        elif hasattr(table_metrics, "items"):
                            # Handle iterable table metrics
                            for table_name, table_metric in table_metrics.items():
                                table_names.add(str(table_name))
                                if hasattr(table_metric, "items_count"):
                                    total_rows += getattr(
                                        table_metric, "items_count", 0
                                    )

        # Set span attributes
        if schema_name:
            span.set_attribute("extract.schema_name", schema_name)
        span.set_attribute("extract.rows", total_rows)
        if resource_names:
            span.set_attribute("extract.resources", ",".join(sorted(resource_names)))
            span.set_attribute("extract.resource_count", len(resource_names))
        if table_names:
            span.set_attribute("extract.tables", ",".join(sorted(table_names)))
            span.set_attribute("extract.table_count", len(table_names))

        # Record metrics
        if total_rows > 0:
            self.rows_counter.add(
                total_rows,
                attributes={
                    "pipeline.name": pipeline.pipeline_name,
                    "step": "extract",
                },
            )

        dlt_logger.debug(
            f"Extract: {total_rows} rows from {len(resource_names)} resources, "
            f"{len(table_names)} tables"
        )

        # Load IDs
        if hasattr(extract_info, "loads_ids") and extract_info.loads_ids:
            loads_ids = extract_info.loads_ids
            if isinstance(loads_ids, list) and loads_ids:
                span.set_attribute("extract.load_id", loads_ids[0])
            elif loads_ids:
                span.set_attribute("extract.load_id", str(loads_ids))

    def _handle_load_step(
        self,
        span: "Span",
        load_info: Any,
        pipeline: "SupportsPipeline",
        trace: "PipelineTrace",
    ) -> None:
        """
        Extract comprehensive metrics from LoadInfo.

        Similar to LogCollector, this method accesses all available metrics
        from LoadInfo, including load_packages, jobs, and metrics structures.
        """
        if not load_info:
            return

        total_rows = 0
        table_names = set()
        total_completed = 0
        total_failed = 0
        load_package_count = 0

        # Method 1: Get row counts from the trace's normalize info (most reliable)
        if trace and hasattr(trace, "last_normalize_info"):
            normalize_info = trace.last_normalize_info
            if normalize_info and hasattr(normalize_info, "row_counts"):
                row_counts = normalize_info.row_counts
                if isinstance(row_counts, dict):
                    total_rows = sum(row_counts.values())
                    table_names.update(row_counts.keys())

        # Method 2: Extract from load_packages (for job counts, table names, and row counts if Method 1 didn't find any)
        # Track whether we need to extract row counts from load packages
        rows_from_normalize = total_rows > 0

        if hasattr(load_info, "load_packages"):
            for package in load_info.load_packages:
                load_package_count += 1

                # Extract row counts from job_file_info only if Method 1 didn't find rows
                if not rows_from_normalize and hasattr(package, "job_file_info"):
                    job_file_info = package.job_file_info
                    if isinstance(job_file_info, dict):
                        for job_info in job_file_info.values():
                            if hasattr(job_info, "items_count"):
                                total_rows += getattr(job_info, "items_count", 0)
                            elif isinstance(job_info, dict):
                                total_rows += job_info.get("items_count", 0)

                # Extract job counts (always needed for observability)
                if hasattr(package, "jobs"):
                    jobs = package.jobs
                    # Handle both dict and object access
                    if isinstance(jobs, dict):
                        completed_jobs = jobs.get("completed_jobs", [])
                        failed_jobs = jobs.get("failed_jobs", [])
                    elif hasattr(jobs, "completed_jobs"):
                        completed_jobs = getattr(jobs, "completed_jobs", [])
                        failed_jobs = getattr(jobs, "failed_jobs", [])
                    else:
                        completed_jobs = []
                        failed_jobs = []

                    total_completed += len(completed_jobs) if completed_jobs else 0
                    total_failed += len(failed_jobs) if failed_jobs else 0

                    # Extract row counts from completed jobs only if Method 1 didn't find rows
                    if not rows_from_normalize:
                        for job in completed_jobs:
                            if hasattr(job, "file_info") and job.file_info:
                                file_info = job.file_info
                                if hasattr(file_info, "items_count"):
                                    total_rows += getattr(file_info, "items_count", 0)

                # Get table names from schema updates
                if hasattr(package, "schema_update"):
                    schema_update = package.schema_update
                    if isinstance(schema_update, dict):
                        table_names.update(schema_update.keys())
                    elif hasattr(schema_update, "keys"):
                        table_names.update(schema_update.keys())

            span.set_attribute("load.jobs.completed", total_completed)
            span.set_attribute("load.jobs.failed", total_failed)
            span.set_attribute("load.packages", load_package_count)
            span.set_attribute("load.tables", len(table_names))

            if table_names:
                span.set_attribute("load.table_names", ",".join(sorted(table_names)))

            if len(table_names) > 0:
                self.tables_counter.add(
                    len(table_names),
                    attributes={
                        "pipeline.name": pipeline.pipeline_name,
                    },
                )

        # Method 3: Try metrics structure (fallback)
        if (
            total_rows == 0
            and hasattr(load_info, "metrics")
            and isinstance(load_info.metrics, dict)
        ):
            for load_metrics_list in load_info.metrics.values():
                if isinstance(load_metrics_list, list):
                    for load_metric in load_metrics_list:
                        if (
                            isinstance(load_metric, dict)
                            and "job_metrics" in load_metric
                        ):
                            job_metrics = load_metric["job_metrics"]
                            if isinstance(job_metrics, dict):
                                for job_metric in job_metrics.values():
                                    if isinstance(job_metric, dict):
                                        total_rows += job_metric.get("items_count", 0)
                                    elif hasattr(job_metric, "items_count"):
                                        total_rows += getattr(
                                            job_metric, "items_count", 0
                                        )

        # Set row count attribute and record metric
        span.set_attribute("load.rows", total_rows)
        if total_rows > 0:
            self.rows_counter.add(
                total_rows,
                attributes={
                    "pipeline.name": pipeline.pipeline_name,
                    "step": "load",
                },
            )

        # Destination info
        if hasattr(load_info, "destination_name"):
            destination_name = getattr(load_info, "destination_name", None)
            if destination_name:
                span.set_attribute("load.destination", str(destination_name))

        if hasattr(load_info, "dataset_name"):
            dataset_name = getattr(load_info, "dataset_name", None)
            if dataset_name:
                span.set_attribute("load.dataset", str(dataset_name))

        dlt_logger.debug(
            f"Load: {total_rows} rows, {len(table_names)} tables, "
            f"{total_completed} completed jobs, {total_failed} failed jobs"
        )

        # Load IDs
        if hasattr(load_info, "loads_ids") and load_info.loads_ids:
            loads_ids = load_info.loads_ids
            if isinstance(loads_ids, list) and loads_ids:
                span.set_attribute("load.load_id", loads_ids[0])
            elif loads_ids:
                span.set_attribute("load.load_id", str(loads_ids))

    def on_end_trace(
        self, trace: "PipelineTrace", pipeline: "SupportsPipeline", send_state: bool
    ) -> None:
        """
        Called when pipeline trace completes.

        Finalizes the root span with aggregate metrics.
        """
        if not self._root_span:
            return

        try:
            success = True

            # Set final pipeline attributes from trace
            if hasattr(trace, "last_normalize_info") and trace.last_normalize_info:
                normalize_info = trace.last_normalize_info
                if hasattr(normalize_info, "row_counts"):
                    row_counts = normalize_info.row_counts
                    total_rows = sum(row_counts.values()) if row_counts else 0

                    self._root_span.set_attribute("pipeline.total_rows", total_rows)
                    self._root_span.set_attribute("pipeline.tables", len(row_counts))

                    # Record final row count metric
                    self.rows_counter.add(
                        total_rows,
                        attributes={
                            "pipeline.name": pipeline.pipeline_name,
                            "step": "run",
                        },
                    )

            # Check for failures in steps
            if hasattr(trace, "steps"):
                for step in trace.steps:
                    if step.step_exception:
                        success = False
                        break

                # Calculate total duration
                if trace.steps:
                    first_start = min(s.started_at for s in trace.steps)
                    last_end = max(s.finished_at for s in trace.steps)
                    total_duration = (last_end - first_start).total_seconds()
                    self._root_span.set_attribute("pipeline.duration", total_duration)

            # Mark success/failure
            self._root_span.set_attribute("pipeline.success", success)

            if success:
                self._root_span.set_status(self._Status(self._StatusCode.OK))
            else:
                self._root_span.set_status(
                    self._Status(self._StatusCode.ERROR, "Pipeline had failures")
                )

            # Record pipeline run metric
            destination_name = "unknown"
            if hasattr(pipeline, "destination") and pipeline.destination:
                destination_name = getattr(
                    pipeline.destination, "destination_name", "unknown"
                )

            self.pipeline_runs_counter.add(
                1,
                attributes={
                    "pipeline.name": pipeline.pipeline_name,
                    "destination": destination_name,
                    "success": str(success),
                },
            )

            dlt_logger.info(
                f"Pipeline trace completed: {pipeline.pipeline_name} "
                f"(success={success})"
            )

        except Exception as e:
            dlt_logger.error(f"Error ending pipeline trace: {e}", exc_info=True)
            self._root_span.record_exception(e)
            self._root_span.set_status(self._Status(self._StatusCode.ERROR, str(e)))

        finally:
            # Detach the root context to clean up
            if self._root_token is not None:
                self._otel_context.detach(self._root_token)
                self._root_token = None

            # End root span (guard against None for defensive coding)
            if self._root_span is not None:
                self._root_span.end()
                self._root_span = None
            self._root_context = None


NULL_COLLECTOR = NullCollector()
