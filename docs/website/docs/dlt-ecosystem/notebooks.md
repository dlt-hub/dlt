---
title: dlt in notebooks
description: Run dlt in notebooks like Colab, Databricks or Jupyter
keywords: [notebook, jupyter]
---
# dlt in notebooks

## Colab
You'll need to install `dlt` like any other dependency:
```sh
!pip install dlt
```

You can configure secrets using **Secrets** sidebar. Just create a variable with the name `secrets.toml` and paste
the content of the **toml** file from your `.dlt` folder into it. We support `config.toml` variable as well.

:::note
`dlt` will not reload the secrets automatically. Please restart your interpreter in Colab options when you add/change
content of the variables above.
:::

## Streamlit
`dlt` will look for `secrets.toml` and `config.toml` in the `.dlt` folder. If `secrets.toml` are not found, it will use
`secrets.toml` from `.streamlit` folder.
If you run locally, maintain your usual `.dlt` folder. When running on streamlit cloud, paste the content of `dlt`
`secrets.toml` into the `streamlit` secrets.

