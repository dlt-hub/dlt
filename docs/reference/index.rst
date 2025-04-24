API reference
=============

.. module:: dlt

This section provides detailed API documentation for the public interfaces of the dlt library.

Quick links
-----------

**Pipeline management**

* :func:`dlt.pipeline` - Create a new pipeline instance
* :meth:`dlt.pipeline.Pipeline.run` - Run a pipeline
* :func:`dlt.run` - Shortcut to run a pipeline with data
* :func:`dlt.attach` - Attach to an existing pipeline

**Source definition decorators**

* :func:`dlt.source` - Decorator to define a data source
* :func:`dlt.resource` - Decorator to define a data resource

**Configuration**

* :func:`dlt.config` - Access configuration values
* :func:`dlt.secrets` - Access secret values

**State management**

* :func:`dlt.state` - Access state dictionary

Core modules
------------

.. toctree::
   :maxdepth: 2

   pipeline/index
   extract/index
   schema/index
   sources/index
   destinations/index
   configuration/index
   state/index
   common/index
   normalize/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`