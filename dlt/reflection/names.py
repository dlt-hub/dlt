import inspect

import dlt
import dlt.destinations
from dlt import pipeline, attach, run, source, resource

DLT = dlt.__name__
DESTINATIONS = dlt.destinations.__name__
PIPELINE = pipeline.__name__
ATTACH = attach.__name__
RUN = run.__name__
SOURCE = source.__name__
RESOURCE = resource.__name__

DETECTED_FUNCTIONS = [PIPELINE, SOURCE, RESOURCE, RUN]
SIGNATURES = {
    PIPELINE: inspect.signature(pipeline),
    ATTACH: inspect.signature(attach),
    RUN: inspect.signature(run),
    SOURCE: inspect.signature(source),
    RESOURCE: inspect.signature(resource)
}