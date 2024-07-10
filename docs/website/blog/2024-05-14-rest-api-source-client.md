---
slug: rest-api-source-client
title: "Announcing: REST API Source toolkit from dltHub - A Python-only high level approach to pipelines"
image:  https://storage.googleapis.com/dlt-blog-images/rest-img.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [rest-api, declarative etl]
---

## What is the REST API Source toolkit?
:::tip
tl;dr: You are probably familiar with REST APIs.

- Our new **REST API Source** is a short, declarative configuration driven way of creating sources.
- Our new **REST API Client** is a collection of Python helpers used by the above source, which you can also use as a standalone, config-free, imperative high-level abstraction for building pipelines.

Want to skip to docs? Links at the [bottom of the post.](#next-steps)
:::

### Why REST configuration pipeline? Obviously, we need one!

But of course! Why repeat write all this code for requests and loading, when we could write it once and re-use it with different APIs with different configs?

Once you have built a few pipelines from REST APIs, you can recognise we could, instead of writing code, write configuration.

**We can call such an obvious next step in ETL tools a “[focal point](https://en.wikipedia.org/wiki/Focal_point_(game_theory))” of “[convergent evolution](https://en.wikipedia.org/wiki/Convergent_evolution)”.**

And if you’ve been in a few larger more mature companies, you will have seen a variety of home-grown solutions that look similar. You might also have seen such solutions as commercial products or offerings.

### But ours will be better…

So far we have seen many REST API configurators and products — they suffer from predictable flaws:

- Local homebrewed flavors are local for a reason: They aren’t suitable for the broad audience. And often if you ask the users/beneficiaries of these frameworks, they will sometimes argue that they aren’t suitable for anyone at all.
- Commercial products are yet another data product that doesn’t plug into your stack, brings black boxes and removes autonomy, so they simply aren’t an acceptable solution in many cases.

So how can `dlt` do better?

Because it can keep the best of both worlds: the autonomy of a library, the quality of a commercial product.

As you will see further, we created not just a standalone “configuration-based source builder” but we also expose the REST API client used enabling its use directly in code.

## Hey community, you made us do it!

The push for this is coming from you, the community. While we had considered the concept before, there were many things `dlt` needed before creating a new way to build pipelines. A declarative extractor after all, would not make `dlt` easier to adopt, because a declarative approach requires more upfront knowledge.

Credits:

- So, thank you Alex Butler for building a first version of this and donating it to us back in August ‘23:  https://github.com/dlt-hub/dlt-init-openapi/pull/2.
- And thank you Francesco Mucio and Willi Müller for re-opening the topic, and creating video [tutorials](https://www.youtube.com/playlist?list=PLpTgUMBCn15rs2NkB4ise780UxLKImZTh).
- And last but not least, thank you to `dlt` team’s Anton Burnashev (also known for [gspread](https://github.com/burnash/gspread) library) for building it out!

## The outcome? Two Python-only interfaces, one declarative, one imperative.

- **dlt’s REST API Source** is a Python dictionary-first declarative source builder, that has enhanced flexibility, supports callable passes, native config validations via python dictionaries, and composability directly in your scripts. It enables generating sources dynamically during runtime, enabling straightforward, manual or automated workflows for adapting sources to changes.
- **dlt’s REST API Client** is the low-level abstraction that powers the REST API Source. You can use it in your imperative code for more automation and brevity, if you do not wish to use the higher level declarative interface.

### Useful for those who frequently build new pipelines

If you are on a team with 2-3 pipelines that never change much you likely won’t see much benefit from our latest tool.
What we observe from early feedback a declarative extractor is great at is enabling easier work at scale.
We heard excitement about the **REST API Source** from:

- companies with many pipelines that frequently create new pipelines,
- data platform teams,
- freelancers and agencies,
- folks who want to generate pipelines with LLMs and need a simple interface.

## How to use the REST API Source?

Since this is a declarative interface, we can’t make things up as we go along, and instead need to understand what we want to do upfront and declare that.

In some cases, we might not have the information upfront, so we will show you how to get that info during your development workflow.

Depending on how you learn better, you can either watch the videos that our community members made, or follow the walkthrough below.

## **Video walkthroughs**

In these videos, you will learn at a leisurely pace how to use the new interface.
[Playlist link.](https://www.youtube.com/playlist?list=PLpTgUMBCn15rs2NkB4ise780UxLKImZTh)
<iframe width="560" height="315" src="https://www.youtube.com/embed/-ejqquY_u20?si=q41I76swYwFpWVSf" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

## Workflow walkthrough: Step by step

If you prefer to do things at your own pace, try the workflow walkthrough, which will show you the workflow of using the declarative interface.

In the example below, we will show how to create an API integration with 2 endpoints. One of these is a child resource, using the data from the parent endpoint to make a new request.

### Configuration Checklist: Before getting started

In the following, we will use the GitHub API as an example.

We will also provide links to examples from this [Google Colab tutorial.](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=SCr8ACUtyfBN&forceEdit=true&sandboxMode=true)


1. Collect your api url and endpoints, [Colab example](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=bKthJGV6Mg6C):
    - An URL is the base of the request, for example: `https://api.github.com/`.
    - An endpoint is the path of an individual resource such as:
        - `/repos/{OWNER}/{REPO}/issues`;
        - or  `/repos/{OWNER}/{REPO}/issues/{issue_number}/comments` which would require the issue number from the above endpoint;
        - or `/users/{username}/starred` etc.
2. Identify the authentication methods, [Colab example](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=mViSDre8McI7):
    - GitHub uses bearer tokens for auth, but we can also skip it for public endpoints https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api?apiVersion=2022-11-28.
3. Identify if you have any dependent request patterns such as first get ids in a list, then use id for requesting details.
   For GitHub, we might do the below or any other dependent requests. [Colab example.](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=vw7JJ0BlpFyh):
      1. Get all repos of an org `https://api.github.com/orgs/{org}/repos`.
      2. Then get all contributors `https://api.github.com/repos/{owner}/{repo}/contributors`.

4. How does pagination work? Is there any? Do we know the exact pattern? [Colab example.](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=rqqJhUoCB9F3)
    - On GitHub, we have consistent [pagination](https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2022-11-28) between endpoints that looks like this `link_header = response.headers.get('Link', None)`.
5. Identify the necessary information for incremental loading, [Colab example](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=fsd_SPZD7nBj):
    - Will any endpoints be loaded incrementally?
    - What columns will you use for incremental extraction and loading?
    - GitHub example: We can extract new issues by requesting issues after a particular time: `https://api.github.com/repos/{repo_owner}/{repo_name}/issues?since={since}`.

### Configuration Checklist: Checking responses during development

1. Data path:
    - You could print the source and see what is yielded. [Colab example.](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=oJ9uWLb8ZYto&line=6&uniqifier=1)
2. Unless you had full documentation at point 4 (which we did), you likely need to still figure out some details on how pagination works.
    1. To do that, we suggest using `curl` or a second python script to do a request and inspect the response. This gives you flexibility to try anything. [Colab example.](https://colab.research.google.com/drive/1qnzIM2N4iUL8AOX1oBUypzwoM3Hj5hhG#scrollTo=tFZ3SrZIMTKH)
    2. Or you could print the source as above - but if there is metadata in headers etc, you might miss it.

### Applying the configuration

Here’s what a configured example could look like:

1. Base URL and endpoints.
2. Authentication.
3. Pagination.
4. Incremental configuration.
5. Dependent resource (child) configuration.

If you are using a narrow screen, scroll the snippet below to look for the numbers designating each component `(n)`.

```py
# This source has 2 resources:
# - issues: Parent resource, retrieves issues incl. issue number
# - issues_comments: Child resource which needs the issue number from parent.

import os
from rest_api import RESTAPIConfig

github_config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com/repos/dlt-hub/dlt/",      #(1)
        # Optional auth for improving rate limits
        # "auth": {                                                   #(2)
        #     "token": os.environ.get('GITHUB_TOKEN'),
        # },
    },
      # The paginator is autodetected, but we can pass it explicitly  #(3)
      #  "paginator": {
      #      "type": "header_link",
      #      "next_url_path": "paging.link",
      #  }
    # We can declare generic settings in one place
    # Our data is stateful so we load it incrementally by merging on id
    "resource_defaults": {
        "primary_key": "id",                                          #(4)
        "write_disposition": "merge",                                 #(4)
        # these are request params specific to GitHub
        "endpoint": {
            "params": {
                "per_page": 10,
            },
        },
    },
    "resources": [
	    # This is the first resource - issues
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",                                     #(1)
                "params": {
                      "sort": "updated",
                      "direction": "desc",
                      "state": "open",
                      "since": {
                            "type": "incremental",                    #(4)
                            "cursor_path": "updated_at",              #(4)
                            "initial_value": "2024-01-25T11:21:28Z",  #(4)
                        },
                }
            },
        },
        # Configuration for fetching comments on issues              #(5)
        # This is a child resource - as in, it needs something from another
        {
            "name": "issue_comments",
            "endpoint": {
                "path": "issues/{issue_number}/comments",            #(1)
                # For child resources, you can use values from the parent resource for params.
                "params": {
                    "issue_number": {
                        # Use type "resolve" to define child endpoint wich should be resolved
                        "type": "resolve",
                        # Parent endpoint
                        "resource": "issues",
                        # The specific field in the issues resource to use for resolution
                        "field": "number",
                    }
                },
            },
            # A list of fields, from the parent resource, which will be included in the child resource output.
            "include_from_parent": ["id"],
        },
    ],
}
```

## And that’s a wrap — what else should you know?

- As we mentioned, there’s also a **REST Client** - an imperative way to use the same abstractions, for example, the auto-paginator - check out this runnable snippet:

    ```py
    from dlt.sources.helpers.rest_client import RESTClient

    # Initialize the RESTClient with the Pokémon API base URL
    client = RESTClient(base_url="https://pokeapi.co/api/v2")

    # Using the paginate method to automatically handle pagination
    for page in client.paginate("/pokemon"):
        print(page)
    ```

- We are going to generate a bunch of sources from OpenAPI specs — stay tuned for an update in a couple of weeks!

## Next steps
- Share back your work! Instructions: **[dltHub-Community-Sources-Snippets](https://www.notion.so/7a7f7ddb39334743b1ba3debbdfb8d7f?pvs=21)**
- Read more about the
    - **[REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)** and
    - **[REST API Client](https://dlthub.com/docs/general-usage/http/rest-client),**
    - and the related **[API helpers](https://dlthub.com/devel/general-usage/http/overview)** and **[requests](https://dlthub.com/docs/general-usage/http/requests)** helper.
- **[Join our community](https://dlthub.com/community)** and give us feedback!
