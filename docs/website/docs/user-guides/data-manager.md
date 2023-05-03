# Data Manager

## Why do we have data platform engineers?

Today, the data platform engineer role is critical to organisations that wish to build and maintain big data products used by other data professionals.

These platform engineers are responsible for building and maintaining the data platforms that enable organizations to collect, store, and analyze data at scale, and their work is essential to the success of data-driven initiatives.

Historically this role would be fulfilled by a cross functional data manager or technical manager. Nowadays as requirements, data, possible technologies and teams grow larger, data products often have a dedicated platform engineers.

## A data platform engineer ensures the data team has the technical resources to operate effectively

A data platform engineer has to ensure that the rest of the data professionals are enabled to deliver on the goals of the business. They often have to come up with a plan to bring a company higher on the data maturity scale. While their job title sounds like an engineer, they have to solve just as much for the human element. Their responsibilities are more along the lines of:

- Understand the goal, budget, technology and staffing constraints and create a plan to reach the goals
- Choose and and govern the development paradigm - Which roles may do what, how, ensure neither bottlenecks nor anarchy are created.
- Design and manage the architecture and infrastructure the team will use,
- Managing data security and access paradigms.
- Educate the team how to use the platform, ensure that the team is able to deliver their goals on the platform.

# II. Why dlt is the cheapest most extensive data loading option that scales with your team and usage

## From simple complex, dlt offers different levels of abstraction to enable and empower your analysts/data scientists, data engineers, pipeline/analytics engineers in building:

### 1. Single use case driven pipelines


* WHAT: Some use cases require specific bits of external data (For example,  attribution might use weather data).

* WHO: These pipelines are usually built by analysts/data scientists and since they support a single use case usually have less business impact if they fail.

* Example: [colab duckdb](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)

### 2. Core business pipelines

* WHAT: These are usually very well engineered to ensure high SLA for all core business cases.

* WHO: The core pipelines are built by data engineers
* Example: [Google sheets pipeline to copy ranges, named ranges or entire documents dynamically](https://dlthub.com/docs/pipelines/google_sheets)

### 3. Customised pipelines

* WHAT: Heavily customised pipelines that give very fine grain control over everything about the data processing and loading. These are usually engineered to solve specific obstacles to data usage, such as performance or cost of loading, or things about the data such as anonymization or managing schemas.

* WHO: Depending if the problem being solved is an engineering or business logic problem, it may be solved by either a data engineer or a business facing analytics engineer.

* Example: [Zendesk pipeline with automatic field renames and optionally pivoted tables for easy analysis](../pipelines/zendesk). Additional customisation example: [pseudonymizing_columns](./customizations/customizing-pipelines/pseudonymizing_columns)

##  ## dlt massively reduces pipeline maintenance in your organisation. With dlt data pipeline maintenance stops being an issue for data engineering. Issues can be directly solved by pythonista stakeholders.

Dlt was built as a productivity tool, so it automates time consuming tasks. Let’s look at the 3 major causes of pipeline maintenance, how dlt has been designed to address each and how it empowers stakeholders to solve them.

- Source produces new data
    - Usual: We do not find out until the stakeholder finds our and requests the data. Then the request goes to the producer, who tells the data engineer what to load, who changes the loading pipeline and notifies the downsteam analytics engineer to pick up the data and include it in reporting. Time to value: Days to Never.
    - dlt way: New field is added and the data alerts channel gets a notification of new fields. The analytics engineer sees the alert and quickly checks with the stakeholders or the producers if they wish this to be reported on. Since the data is already loaded, the analyst can adjust the sql and deploy changes same day.
- Source has a new endpoint
    - Usual: Data pipeline engineer needs to add code to extract normalise and load the new endpoint. Complexity: Need to read and edit existing code or do extract normalise and load from scratch.
    - dlt way: We encourage dynamic resource generation, so a new endpoint can probably be added to the list of endpoints, and if the api is self consistent the data will be loaded. Alternatively, the developer will need to figure out how to request the new data and pass it to dlt. Complexity: Add 1 word to a list, or do extraction from scratch.
- Business is using a new service
    - Usual way: Data engineer builds a pipeline. They usually build extraction from scratch, normalisation based on what is desired by the business and then finally doing typical loading. Somewhere, data typing happens, either before ingestion, or everything is ingested as string and typed after. After building, we weed out the bugs and gotchas, and finally after a few weeks our pipeline runs securely. All the code will need to be maintained
    - dlt way: We only write minimal code, extraction code. We declare how we load, the typing, normalisation is automated. We only maintain extraction code. This can now be maintained by a layman  - they only need to understand extraction, and not a whole bunch of glue code.


# More open source, less vendor lock.

Dlt was made to solve your problems around data loading without introducing redundancy, complexity, or vendor monetisation hooks.

dlt is the only data loading solution meant to play nice with the rest of the stack. The other solutions either don’t integrate well, or try to replace your stack such as your orchestrator with their inferior solutions.

Other EL solutions are

- Either paid Saas which is expensive, not extensible, not customisable and usually with separate monitoring, alerting, scheduling
- Or a framework that is so complex that now you gotta pay a devops or an agency to the same effect. Singer framework is a 10y old framework that reads like java, while the newer kid on the block is both complex to use, incomplete in design (loads data as strings, putting the maintenance burden on you) and bringing overhead (new runner, new optimisation work, no scaling)

![open core data stack](/img/open-core-data-stack.png)

# Better governance with less human middleware

### Notify schema changes: Autonomy and governance to the producer


When a stakeholder has to request something from another human, the pain of asking becomes an obstacle, and in larger orgs the request chain might prove to be a dead end.

For this reason, there have been attempts to solve such problems through governing paradigm shifts, such as data mesh.

Dlt’s automation of data normalisation at ingestion supports shortcutting the human element - the producer can easily adjust their source schema and the data will be automatically made available downstream in the analytical system, where they or an analyst can use it.

By using dlt’s “notify on schema change” functionality, this can act as a data contract test to notify both the consumer and the producer of any changes. Read more about [alerting schema change](./running-in-production/running#inspect-save-and-alert-on-schema-changes). You can use the schema generated from the data or you can define it, read more about [schema](..general-usage/schema)