---
slug: single-pane-glass
title: "Single pane of glass for pipelines running on various orchestrators"
image: https://storage.googleapis.com/dlt-blog-images/single-pane-glass.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [data observability, data pipeline observability]
---

# The challenge in discussion

In large organisations, there are often many data teams that serve different departments. These data teams usually cannot agree where to run their infrastructure, and everyone ends up doing something else. For example:

- 40 generated GCP projects with various services used on each
- Native AWS services under no particular orchestrator
- That on-prem machine that’s the only gateway to some strange corporate data
- and of course that SaaS orchestrator from the marketing team
- together with the event tracking lambdas from product
- don’t forget the notebooks someone scheduled

So, what’s going on? Where is the data flowing? what data is it?

# The case at hand

At dltHub, we are data people, and use data in our daily work.

One of our sources is our community slack, which we use in 2 ways:

1. We are on free tier Slack, where messages expire quickly. We refer to them in our github issues and plan to use the technical answers for training our GPT helper. For these purposes, we archive the conversations daily. We run this pipeline on github actions ([docs](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-github-actions)) which is a serverless runner that does not have a short time limit like cloud functions.
2. We measure the growth rate of the dlt community - for this, it helps to understand when people join Slack. Because we are on free tier, we cannot request this information from the API, but can capture the event via a webhook. This runs serverless on cloud functions, set up as in this [documentation](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook).

So already we have 2 different serverless run environments, each with their own “run reporting”.

Not fun to manage. So how do we achieve a single pane of glass?

### Alerts are better than monitoring

Since “checking” things can be tedious, we rather forget about it and be notified. For this, we can use slack to send messages. Docs [here](https://dlthub.com/docs/running-in-production/running#using-slack-to-send-messages).

Here’s a gist of how to use it

```py
from dlt.common.runtime.slack import send_slack_message

def run_pipeline_and_notify(pipeline, data):
    try:
        load_info = pipeline.run(data)
    except Exception as e:
        send_slack_message(
            pipeline.runtime_config.slack_incoming_hook,
						f"Pipeline {pipeline.pipeline_name} failed! \n Error: {str(e)}")
        raise
```

### Monitoring load metrics is cheaper than scanning entire data sets

As for monitoring, we could always run some queries to count the amount of loaded rows ad hoc - but this would scan a lot of data and cost significantly on larger volumes.

A better way would be to leverage runtime metrics collected by the pipeline such as row counts. You can find docs on how to do that [here](https://dlthub.com/docs/running-in-production/monitoring#data-monitoring).

### If we care, governance is doable too

Now, not everything needs to be governed. But for the slack pipelines we want to tag which columns have personally identifiable information, so we can delete that information and stay compliant.

One simple way to stay compliant is to annotate your raw data schema and use views for the transformed data, so if you delete the data at source, it’s gone everywhere.

If you are materialising your transformed tables, you would need to have column level lineage in the transform layer to facilitate the documentation and deletion of the data. [Here’s](https://dlthub.com/docs/blog/dlt-lineage-support) a write up of how to capture that info. There are also other ways to grab a schema and annotate it, read more [here](https://dlthub.com/docs/general-usage/schema).

# In conclusion

There are many reasons why you’d end up running pipelines in different places, from organisational disagreements, to skillset differences, or simply technical restrictions.

Having a single pane of glass is not just beneficial but essential for operational coherence.

While solutions exist for different parts of this problem, the data collection still needs to be standardised and supported across different locations.

By using a tool like dlt, standardisation is introduced with ingestion, enabling cross-orchestrator observability and monitoring.

### Want to discuss?

[Join our slack community](https://dlthub.com/community) to take part in the conversation.