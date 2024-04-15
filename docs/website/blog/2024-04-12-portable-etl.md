---
slug: portable-elt
title: "Portable, embeddable ETL - what if pipelines could run anywhere?"
image:  https://storage.googleapis.com/dlt-blog-images/embeddable-etl.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [full code etl, yes code etl, etl, pythonic]
---

# Portable, embeddable ETL - what if pipelines could run anywhere?

![embeddable etl](https://storage.googleapis.com/dlt-blog-images/embeddable-etl.png)

## The versatility that enables "one way to rule them all"... requires a devtool

A unified approach to ETL processes centers around standardization without compromising flexibility.
To achieve this, we need to be enabled to build and run custom code, bu also have helpers to enable us to standardise and simplify our work.

In the data space, we have a few custom code options, some of which portable. But what is needed to achieve
universality and portability is more than just a code standard.

So what do we expect from such a tool?
- It should be created for our developers
- it should be easily pluggable into existing tools and workflows
- it should perform across a variety of hardware and environments.

## Data teams don't speak Object Oriented Programming (OOP)

Connectors are nice, but when don't exist or break, what do we do? We need to be able to build and maintain those connectors simply, as we work with the rest of our scripts.

The data person has a very mixed spectrum of activities and responsibilities, and programming is often a minor one. Thus, across a data team, while some members
can read or even speak OOP, the team will not be able to do so without sacrificing other capabilities.

This means that in order to be able to cater to a data team as a dev team, we need to aknowledge a different abstraction is needed.

### Goodbye OOP, hello `@decorators`!

Data teams often navigate complex systems and workflows that prioritize functional clarity over object-oriented
programming (OOP) principles. They require tools that simplify process definition, enabling quick, readable,
and maintainable data transformation and movement. Decorators serve this purpose well, providing a straightforward
way to extend functionality without the overhead of class hierarchies and inheritance.

Decorators in Python allow data teams to annotate functions with metadata and operational characteristics,
effectively wrapping additional behavior around core logic. This approach aligns with the procedural mindset
commonly found in data workflows, where the emphasis is on the transformation steps and data flow rather than the objects that encapsulate them.

By leveraging decorators, data engineers can focus on defining what each part of the ETL process does—extract,
transform, load—without delving into the complexities of OOP. This simplification makes the code more accessible
to professionals who may not be OOP experts but are deeply involved in the practicalities of data handling and analysis.

## The ability to run embedded is more than just scalability

Most traditional ETL frameworks are architected with the assumption of relatively abundant computational resources.
This makes sense given the resource-intensive nature of ETL tasks when dealing with massive datasets.

However, this assumption often overlooks the potential for running these processes on smaller, more constrained infrastructures,
such as directly embedded within an orchestrator or on edge devices.

The perspective that ETL processes necessarily require large-scale infrastructure is ripe for challenge. In fact,
there is a compelling argument to be made for the efficiency and simplicity of executing ETL tasks, particularly web
requests for data integration, on smaller systems. This approach can offer significant cost savings and agility,
especially when dealing with less intensive data loads or when seeking to maintain a smaller digital footprint.

Small infrastructure ETL runs can be particularly efficient in situations where real-time data processing is not
required, or where data volumes are modest. By utilizing the orchestrator's inherent scheduling and management
capabilities, one can execute ETL jobs in a leaner, more cost-effective manner. This can be an excellent fit for
organizations that have variable data processing needs, where the infrastructure can scale down to match lower demands,
thereby avoiding the costs associated with maintaining larger, underutilized systems.

### Running on small workers is easier than spinning up infra

Running ETL processes directly on an orchestrator can simplify architecture by reducing the number of
moving parts and dependencies. It allows data teams to quickly integrate new data sources and destinations with minimal
overhead. This methodology promotes a more agile and responsive data architecture, enabling businesses to adapt more swiftly
to changing data requirements.

It's important to recognize that this lean approach won't be suitable for all scenarios, particularly where data volumes
are large or where the complexity of transformations requires the robust computational capabilities of larger systems.
Nevertheless, for a significant subset of ETL tasks, particularly those involving straightforward data integrations via web requests,
running on smaller infrastructures presents an appealing alternative that is both cost-effective and simplifies the
overall data processing landscape.

### Dealing with spiky loads is easier on highly parallel infras like serverless functions

Serverless functions are particularly adept at managing spiky data loads due to their highly parallel and elastic nature.
These platforms automatically scale up to handle bursts of data requests and scale down immediately after processing,
ensuring that resources are utilized only when necessary. This dynamic scaling not only improves resource efficiency
but also reduces costs, as billing is based on actual usage rather than reserved capacity.

The stateless design of serverless functions allows them to process multiple, independent tasks concurrently.
This capability is crucial for handling simultaneous data streams during peak times, facilitating rapid data processing
that aligns with sudden increases in load. Each function operates in isolation, mitigating the risk of one process impacting another,
which enhances overall system reliability and performance.

Moreover, serverless architectures eliminate the need for ongoing server management and capacity planning.
Data engineers can focus solely on the development of ETL logic without concerning themselves with underlying infrastructure issues.
This shift away from operational overhead to pure development accelerates deployment cycles and fosters innovation.

## Some examples of embedded portability with dlt

### Dagster's embedded ETL now supports `dlt` - enabling devs to do what they love - build.

The "Stop Reinventing Orchestration: Embedded ELT in the Orchestrator" blog post by Pedram from Dagster Labs,
introduces the concept of Embedded ELT within an orchestration framework, highlighting the transition in data engineering from bulky,
complex systems towards more streamlined, embedded solutions that simplify data ingestion and management. This evolution is seen in
the move away from heavy tools like Airbyte or Meltano towards utilizing lightweight, performant libraries which integrate seamlessly into existing
orchestration platforms, reducing deployment complexity and operational overhead. This approach leverages the inherent capabilities of
orchestration systems to handle concerns typical to data ingestion, such as state management, error handling, and observability,
thereby enhancing efficiency and developer experience.

dlt was built for just such a scenario and we are happy to be adopted into it. Besides adding connectors, dlt adds a simple way to build custom pipelines.

### Dagworks' `dlt` + `duckdb` + `ibis` + `Hamilton` demo

The DAGWorks Substack post introduces a highly portable pipeline of all libraries, and leverages a blend of open-source Python libraries: dlt, Ibis, and Hamilton.
This integration exemplifies the trend towards modular, decentralized data systems, where each component specializes in a segment of the data handling process—dlt for extraction and loading,
Ibis for transformation, and Hamilton for orchestrating complex data flows. These technologies are not just tools but represent a
paradigm shift in data engineering, promoting agility, scalability, and cost-efficiency in deploying serverless microservices.

The post not only highlights the technical prowess of combining these libraries to solve practical problems like message
retention and thread summarization on Slack but also delves into the meta aspects of such integrations. It reflects on the broader
implications of adopting a lightweight stack that can operate within diverse infrastructures, from cloud environments to embedded systems,
underscoring the shift towards interoperability and backend agnosticism in data engineering practices. This approach illustrates a shift
in the data landscape, moving from monolithic systems to flexible, adaptive solutions that can meet specific organizational needs
without heavy dependencies or extensive infrastructure.

## Closing words
dlt was designed to be the anti-platform - Portable, embeddable etl made for data devs.
We welcome a growing list of [projects using us](https://github.com/dlt-hub/dlt/network/dependents) :)

Want to discuss? [Join our slack community](https://dlthub.com/community) to take part in the conversation.

