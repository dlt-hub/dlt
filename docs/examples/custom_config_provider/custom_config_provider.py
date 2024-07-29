"""
---
title: Use custom yaml file for config and secrets
description: We show how to keep configuration in yaml file with switchable profiles and simple templates
keywords: [config, yaml config, profiles]
---

This example shows how to replace secrets/config toml files with a yaml file that contains several profiles (prod and dev) and jinja-like
placeholders that are replaced with corresponding env variables.
`dlt` resolves configuration by querying so called config providers (to ie. query env variables or content of a toml file).
Here we will instantiate a provider with a custom loader and register it to be queried. At the end we demonstrate (using mock github source)
that `dlt` uses it along other (standard) providers to resolve configuration.

In this example you will learn to:

* Implement custom configuration loader that parses yaml file, manipulates it and then returns final Python dict
* Instantiate custom provider (CustomLoaderDocProvider) from the loader
* Register provider instance to be queried

"""

import os
import re
import dlt
import yaml
import functools

from dlt.common.configuration.providers import CustomLoaderDocProvider
from dlt.common.utils import map_nested_in_place


# config for all resources found in this file will be grouped in this source level config section
__source_name__ = "github_api"


def eval_placeholder(value):
    """Replaces jinja placeholders {{ PLACEHOLDER }} with environment variables"""
    if isinstance(value, str):

        def replacer(match):
            return os.environ[match.group(1)]

        return re.sub(r"\{\{\s*(\w+)\s*\}\}", replacer, value)
    return value


def loader(profile_name: str):
    """Loads yaml file from profiles.yaml in current working folder, selects profile, replaces
    placeholders with env variables and returns Python dict with final config
    """
    path = os.path.abspath("profiles.yaml")
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    # get the requested environment
    config = config.get(profile_name, None)
    if config is None:
        raise RuntimeError(f"Profile with name {profile_name} not found in {os.path.abspath(path)}")
    # evaluate all placeholders
    # NOTE: this method only works with placeholders wrapped as strings in yaml. use jinja lib for real templating
    return map_nested_in_place(eval_placeholder, config)


@dlt.resource(standalone=True)
def github(url: str = dlt.config.value, api_key=dlt.secrets.value):
    # just return the injected config and secret
    yield url, api_key


if __name__ == "__main__":
    # mock env variables to fill placeholders in profiles.yaml
    os.environ["GITHUB_API_KEY"] = "secret_key"  # mock expected var

    # dlt standard providers work at this point (we have profile name in config.toml)
    profile_name = dlt.config["dlt_config_profile_name"]

    # instantiate custom provider using `prod` profile
    # NOTE: all placeholders (ie. GITHUB_API_KEY) will be evaluated in next line!
    provider = CustomLoaderDocProvider("profiles", functools.partial(loader, profile_name))
    # register provider, it will be added as the last one in chain
    dlt.config.register_provider(provider)

    # your pipeline will now be able to use your yaml provider
    # p = Pipeline(...)
    # p.run(...)

    # show the final config
    print(provider.to_yaml())
    # or if you like toml
    print(provider.to_toml())

    # inject && evaluate resource
    config_vals = list(github())
    print(config_vals)
    assert config_vals[0] == ("https://github.com/api", "secret_key")
