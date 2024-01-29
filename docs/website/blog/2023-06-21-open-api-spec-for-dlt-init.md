---
slug: open-api-spec-for-dlt-init
title: "dlt & openAPI code generation: A step beyond APIs and towards 10,000s of live datasets"
image: https://camo.githubusercontent.com/1aca1132999dde59bc5b274aeb4d01c79eab525941362491a534ddd8d1015dce/68747470733a2f2f63646e2e6c6f6f6d2e636f6d2f73657373696f6e732f7468756d626e61696c732f32383036623837336261316334653065613338326562336234666261663830382d776974682d706c61792e676966
authors:
  name: Matthaus Krzykowski
  title: Co-Founder & CEO at dltHub
  url: https://twitter.com/matthausk
  image_url: https://pbs.twimg.com/profile_images/642282396751130624/9ixo0Opj_400x400.jpg
tags: [fastapi, feature, openapi, dlt pipline, pokeapi]
---
Today we are releasing a proof of concept of the [`dlt init`](https://dlthub.com/docs/walkthroughs/create-a-pipeline) extension that can [generate `dlt` pipelines from an OpenAPI specification.](https://github.com/dlt-hub/dlt-init-openapi)

If you build APIs, for example with [FastAPI](https://fastapi.tiangolo.com/), you can, thanks to the [OpenAPI spec,](https://spec.openapis.org/oas/v3.1.0) automatically generate a [python client](https://pypi.org/project/openapi-python-client/0.6.0a4/) and give it to your users. Our demo takes this a step further and enables you to generate advanced `dlt` pipelines that, in essence, convert your API into a live dataset.

You can see how Marcin generates such a pipeline from the OpenAPI spec using the [Pokemon API](https://pokeapi.co/) in the Loom below.  
[![marcin-demo](https://camo.githubusercontent.com/1aca1132999dde59bc5b274aeb4d01c79eab525941362491a534ddd8d1015dce/68747470733a2f2f63646e2e6c6f6f6d2e636f6d2f73657373696f6e732f7468756d626e61696c732f32383036623837336261316334653065613338326562336234666261663830382d776974682d706c61792e676966)](https://www.loom.com/share/2806b873ba1c4e0ea382eb3b4fbaf808?sid=501add8b-90a0-4734-9620-c6184d840995)  
  
Part of our vision is that each API will come with a `dlt` pipeline - similar to how these days often it comes with a python client. We believe that very often API users do not really want to deal with endpoints, http requests, and JSON responses. They need live, evolving datasets that they can place anywhere they want so that it's accessible to any workflow.

We believe that API builders will bundle `dlt` pipelines with their APIs only if such a process is hassle free. One answer to that is code generation and the reuse of information from the OpenAPI spec.

This release is a part of a bigger vision for `dlt` of a world centered around accessible data for modern data teams. In these new times code is becoming more disposable, but the data stays valuable. We eventually want to create an ecosystem where hundreds of thousands of pipelines will be created, shared, and deployed. Where datasets, reports, and analytics can be written and shared publicly and privately. [Code generation is automation on steroids](https://dlthub.com/product/#code-generation-is-automation-on-steroids) and we are going to be releasing many more features based on this principle.  
  
## Generating a pipeline for PokeAPI using OpenAPI spec  
  
In the embedded loom you saw Marcin pull data from the `dlt` pipeline created from the OpenAPI spec. The proof of concept already uses a few tricks and heuristics to generate useful code. Contrary to what you may think, PokeAPI is a complex one with a lot of linked data types and endpoints!

- It created a resource for all endpoints that return lists of objects.
- It used heuristics to discover and extract lists wrapped in responses.
- It generated dlt transformers from all endpoints that have a matching list resource (and return the same object type).
- It used heuristics to find the right object id to pass to the transformer.
- It allowed Marcin to select endpoints using the [questionary](https://github.com/tmbo/questionary) lib in CLI.
- It listed at the top the endpoints with the most central data types (think of tables that refer to several other tables).

As mentioned, the PoC was well tested with PokeAPI. We know it also works with many other - we just can’t guarantee that our tricks work in all cases as they were not extensively tested.

Anyway: [Try it out yourself!](https://github.com/dlt-hub/dlt-init-openapi)

## We plan to take this even further!

- **We will move this feature into `dlt init` and integrate with LLM code generation!**
- **Restructuring of the python client:** We will fully restructure the underlying python client. We'll compress all the files in the `pokemon/api` folder into a single, nice, and extendable client.
- **GPT-4 friendly:** We'll allow easy addition of pagination and other injections into the client.
- **More heuristics:** Many more heuristics to extract resources, their dependencies, infer the incremental and merge loading.
- **Tight integration with FastAPI on the code level to get even more heuristics!**

[Your feedback and help is greatly appreciated.](https://github.com/dlt-hub/dlt/blob/devel/CONTRIBUTING.md) [Join our community, and let’s build together.](https://dlthub.com/community)