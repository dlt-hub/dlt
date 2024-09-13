---
slug: openapi-generation-chargebee
title: "Saving 75% of work for a Chargebee Custom Source via pipeline code generation with dlt"
image:  https://storage.googleapis.com/dlt-blog-images/openapi-generation.png
authors:
  name: Adrian Brudaru & Violetta Mishechkina
  title: Data Engineer & ML Engineer
  url: https://github.com/dlt-hub/dlt
  image_url: https://avatars.githubusercontent.com/u/89419010?s=48&v=4
tags: [data observability, data pipeline observability, openapi]
---

At dltHub, we have been pioneering the future of data pipeline generation, [making complex processes simple and scalable.](https://dlthub.com/product/#multiply-don't-add-to-our-productivity) We have not only been building dlt for humans, but also LLMs.

Pipeline generation on a simple level is already possible directly in ChatGPT chats - just ask for it. But doing it at scale, correctly, and producing comprehensive, good quality pipelines is a much more complex endeavour.

# Our early exploration with code generation

As LLMs became available at the end of 2023, we were already uniquely positioned to be part of the wave. By being a library, a LLM could use dlt to build pipelines without the complexities of traditional ETL tools.

This raised from the start the question - what are the different levels of pipeline quality? For example, how does a user code snippet, which formerly had value, compare to LLM snippets which can be generated en-masse? What does a perfect pipeline look like now, and what can LLMs do?

We were only able to answer some of these questions, but we had some concrete outcomes that we carry into the future.

### In June ‘23 we added a GPT-4 docs helper that generates snippets

- try it on our docs; it's widely used as code troubleshooter
![gpt-4 dhelp](https://storage.googleapis.com/dlt-blog-images/dhelp.png)

### We created an OpenAPI based pipeline generator

- Blog: https://dlthub.com/docs/blog/open-api-spec-for-dlt-init
- OpenApi spec describes the api; Just as we can create swagger docs or a python api wrapper, we can create pipelines


[![marcin-demo](https://storage.googleapis.com/dlt-blog-images/openapi_loom_old.png)](https://www.loom.com/share/2806b873ba1c4e0ea382eb3b4fbaf808?sid=501add8b-90a0-4734-9620-c6184d840995)



### Running into early limits of LLM automation: A manual last mile is needed

Ideally, we would love to point a tool at an API or doc of the API, and just have the pipeline generated.

However, the OpenApi spec does not contain complete information for generating a complete pipeline. There’s many challenges to overcome and gaps that need to be handled manually.

While LLM automation can handle the bulk, some customisation remains manual, generating requirements towards our ongoing efforts of full automation.

# Why revisit code generation at dlt now?

### Growth drives a need for faster onboarding

The dlt community has been growing steadily in recent months. In February alone we had a 25% growth on Slack and even more in usage.

New users generate a lot of questions and some of them used our onboarding program, where we speed-run users through any obstacles, learning how to make things smoother on the dlt product side along the way.

### Onboarding usually means building a pipeline POC fast

During onboarding, most companies want to understand if dlt fits their use cases. For these purposes, building a POC pipeline is pretty typical.

This is where code generation can prove invaluable - and reducing a build time from 2-3d to 0.5 would lower the workload for both users and our team.
💡 *To join our onboarding program, fill this [form](https://forms.gle/oMgiTqhnrFrYrfyD7) to request a call.*


# **Case Study: How our solution engineer Violetta used our PoC to generate a production-grade  Chargebee pipeline within hours**

In a recent case, one of our users wanted to try dlt with a source we did not list in our [public sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources/) - Chargebee.

Since the Chargebee API uses the OpenAPI standard, we used the OpenAPI PoC dlt pipeline code generator that we built last year.

### Starting resources

POC for getting started, human for the last mile.

- Blog post with a video guide https://dlthub.com/docs/blog/open-api-spec-for-dlt-init
- OpenAPI Proof of Concept pipeline generator: https://github.com/dlt-hub/dlt-init-openapi
- Chargebee openapi spec https://github.com/chargebee/openapi
- Understanding of how to make web requests
- And 4 hours of time - this was part of our new hire Violetta’s onboarding tasks at dltHub so it was her first time using dlt and the code generator.

Onward, let’s look at how our new colleague Violetta, ML Engineer, used this PoC to generate PoCs for our users.

### Violetta shares her experience:

So the first thing I found extremely attractive — the code generator actually created a very simple and clean structure to begin with.

I was able to understand what was happening in each part of the code. What unfortunately differs from one API to another — is the authentication method and pagination. This needed some tuning. Also, there were other minor inconveniences which I needed to handle.

There were no great challenges. The most ~~difficult~~ tedious probably was to manually change pagination in different sources and rename each table.

1) Authentication
The provided Authentication was a bit off. The generated code assumed the using of a username and password but what was actually required was — an empty username + api_key as a password. So super easy fix was changing

```py
def to_http_params(self) -> CredentialsHttpParams:
	cred = f"{self.api_key}:{self.password}" if self.password else f"{self.username}"
	encoded = b64encode(f"{cred}".encode()).decode()
	return dict(cookies={}, headers={"Authorization": "Basic " + encoded}, params={})
```

to

```py
def to_http_params(self) -> CredentialsHttpParams:
  encoded = b64encode(f"{self.api_key}".encode()).decode()
  return dict(cookies={}, headers={"Authorization": "Basic " + encoded}, params={})
```

Also I was pleasantly surprised that generator had several different authentication methods built in and I could easily replace `BasicAuth` with `BearerAuth` of `OAuth2` for example.

2) Pagination

For the code generator it’s hard to guess a pagination method by OpenAPI specification, so the generated code has no pagination 😞. So I had to replace a line

```py
def f():
  yield _build_response(requests.request(**kwargs))
```

  with yielding form a 6-lines `get_page` function

```py
def get_pages(kwargs: Dict[str, Any], data_json_path):
    has_more = True
    while has_more:
        response = _build_response(requests.request(**kwargs))
        yield extract_nested_data(response.parsed, data_json_path)
        kwargs["params"]["offset"] = response.parsed.get("next_offset", None)
        has_more = kwargs["params"]["offset"] is not None
```

The downside — I had to do it for each resource.

3) Too many files

The code wouldn’t run because it wasn’t able to find some models. I found a commented line in generator script

```py
# self._build_models()
```

I regenerated code with uncommented line and understood why it was commented. Code created 224 `.py` files under the `models` directory. Turned out I needed only two of them. Those were models used in api code. So I just removed other 222 garbage files and forgot about them.

4) Namings

The only problem I was left with — namings. The generated table names were like
`ListEventsResponse200ListItem` or `ListInvoicesForACustomerResponse200ListItem` . I had to go and change them to something more appropriate like `events` and `invoices` .

# The result

Result: https://github.com/dlt-hub/chargebee-source

I did a walk-through with our user. Some additional context started to appear. For example, which endpoints needed to be used with `replace` write disposition, which would require specifying the `merge` keys. So in the end this source would still require some testing to be performed and some fine-tuning from the user.
I think the silver lining here is how to start. I don’t know how much time I would’ve spent on this source if I started from scratch. Probably, for the first couple of hours, I would be trying to decide where should the authentication code go, or going through the docs searching for information on how to use dlt configs. I would certainly need to go through all API endpoints in the documentation to be able to find the one I needed. There are a lot of different things which could be difficult especially if you’re doing it for the first time.
I think in the end if I had done it from scratch, I would’ve got cleaner code but spent a couple of days. With the generator, even with finetuning, I spent about half a day. And the structure was already there, so it was overall easier to work with and I didn’t have to consider everything upfront.

### We are currently working on making full generation a reality.

* Stay tuned for more, or
* [Join our slack community](https://dlthub.com/community) to take part in the conversation.