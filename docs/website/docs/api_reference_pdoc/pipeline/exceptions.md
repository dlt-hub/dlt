Module pipeline.exceptions
==========================

Classes
-------

`CannotRestorePipelineException(pipeline_name: str, pipelines_dir: str, reason: str)`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException

`InvalidPipelineName(pipeline_name: str, details: str)`
:   Inappropriate argument value (of correct type).
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.ValueError
    * builtins.Exception
    * builtins.BaseException

`PipelineConfigMissing(pipeline_name: str, config_elem: str, step: Literal['run', 'extract', 'normalize', 'load'])`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException

`PipelineHasPendingDataException(pipeline_name: str, pipelines_dir: str)`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException

`PipelineNotActive(pipeline_name: str)`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException

`PipelineStateEngineNoUpgradePathException(pipeline_name: str, init_engine: int, from_engine: int, to_engine: int)`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException

`PipelineStepFailed(pipeline: dlt.common.pipeline.SupportsPipeline, step: Literal['run', 'extract', 'normalize', 'load'], exception: BaseException, step_info: Any = None)`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException

`SqlClientNotAvailable(pipeline_name: str, destination_name: str)`
:   Common base class for all non-exit exceptions.
    
    Base class for all pipeline exceptions. Should not be raised.

    ### Ancestors (in MRO)

    * dlt.common.exceptions.PipelineException
    * dlt.common.exceptions.DltException
    * builtins.Exception
    * builtins.BaseException
