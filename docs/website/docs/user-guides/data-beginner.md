---
title: Data Beginner
description: A guide to using dlt for aspiring data professionals
keywords: [beginner, analytics, machine learning]
---

# Data Beginner

If you are an aspiring data professional, here are some ways you can showcase your understanding and value to data teams with the help of `dlt`.

## Analytics: Empowering decision-makers

Operational users at a company need general business analytics capabilities to make decisions (e.g. dashboards, data warehouse, self-service, etc.)

### Show you can deliver results, not numbers

The goal of such a project is to get you into the top 5% of candidates, so you get invited to an interview and understand pragmatically what is expected of you.

Depending on whether you want to be more in engineering or analytics, you can focus on different parts of this project. If you showcase that you are able to deliver end to end, there remains little reason for a potential employer to not hire you.

Someone hiring folks on this business analytics path will be looking for the following skills:
- Can you load data to a db?
    - Can you do incremental loading?
    - Are your pipelines maintainable?
    - Are your pipelines reusable? Do they take meaningful arguments?
- Can you transform the data to a standard architecture?
    - Do you know dimensional modelling architecture?
    - Does your model make the data accessible via a user facing tool to a business user?
    - Can you translate a business requirement into a technical requirement?
- Can you identify a use case and prepare reporting?
    - Are you displaying a sensible use case?
    - Are you taking a pragmatic approach as to what should be displayed and why?
    - Did you hard code charts in a notebook that the end user cannot use or did you use a user-facing dashboard tool?
    - Is the user able to answer follow up questions by changing the dimensions in a tool or did you hard code queries?

Project idea:
1. Choose an API that produces data. If this data is somehow business relevant, that’s better. Many business apps offer free developer accounts that allow you to develop business apps with them.
2. Choose a use case for this data. Make sure this use case makes some business sense and is not completely theoretical. Business understanding and pragmatism are key for such roles, so do not waste your chance to show it. Keep the use case simple-otherwise it will not be pragmatic right off the bat, handicapping yourself from a good outcome. A few examples are ranking leads in a sales CRM, clustering users, and something around customer lifetime value predictions.
3. Build a dlt pipeline that loads data from the API for your use case. Keep the case simple and your code clean. Use explicit variable and method names. Tell a story with your code. For loading mode, use incremental loading and don’t hardcode parameters that are subject to change.
4. Build a [dbt package](./using-loaded-data/transforming-the-data) for this pipeline.
5. Build a visualization. Focus on usability more than code. Remember, your goal is to empower a business user to self-serve, so hard coded dashboards are usually seen as liabilities that need to be maintained. On the other hand, dashboard tools can be adjusted by business users too. For example, the free “Looker studio” fro Google is relatable to business users, while notebooks might make them feel insecure. Your evaluator will likely not take time to set up and run your things, so make sure your outcomes are well documented with images. Make sure they are self readable, explain how you intend the business user to use this visualization to fulfil the use case.
6. Make it presentable somewhere public, such as GitHub, and add docs. Show it to someone for feedback. You will find likeminded people in [our Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) that will happily give their opinion.

## Machine Learning: Automating decisions

Solving specific business problems with data products that generate further insights and sometimes automate decisions.

### Show you can solve business problems

Here the challenges might seem different than the business analytics path, but they are often quite similar. Many courses focus on statistics and data science but very few focus on pragmatic approaches to solving business problems in organizations. Most of the time, the largest obstacles to solving a problem with ML are not purely algorithmic but rather about the semantics of the business, data, and people who need to use the data products.

Employers look for a project that showcases both technical ability and business pragmatism in a use case. In reality, data does not typically come in files but via APIs with fresh data, where you usually will have to grab it and move it somewhere to use, so show your ability to deliver end to end.

Project idea:
1. Choose an API that produces data. If this data is somehow business relevant, that’s better. Many business apps offer free developer accounts that allow you to develop business apps with them.
2. Choose a use case for this data. Make sure this use case makes some business sense and is not completely theoretical. Business understanding and pragmatism are key for such roles, so do not waste your chance to show it. Keep the use case simple-otherwise it will not be pragmatic right off the bat, handicapping yourself from a good outcome. A few examples are ranking leads in a sales CRM, clustering users, and something around customer lifetime value predictions.
3. Build a dlt pipeline that loads data from the API for your use case. Keep the case simple and your code clean. Use explicit variable and method names. Tell a story with your code. For loading mode, use incremental loading and don’t hardcode parameters that are subject to change.
4. Build a data model with SQL. If you are ambitious you could try running the SQL with a [dbt package](./using-loaded-data/transforming-the-data)
5. Showcase your chosen use case that uses ML or statistics to achieve your goal. Don’t forget to mention how you plan to do this “in production”. Choose a case that is simple so you don’t end up overcomplicating your solution. Focus on outcomes and next steps. Describe what the company needs to do to use your results, demonstrating that you understand the costs of your propositions.
6. Make it presentable somewhere public, such as GitHub, and add docs. Show it to someone for feedback. You will find likeminded people in [our Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) that will happily give their opinion.

## Further reading

Good docs pages to check out:
- [Getting started](../getting-started)
- [Create a pipeline](../walkthroughs/create-a-pipeline)
- [Run a pipeline](../walkthroughs/run-a-pipeline)
- [Deploy a pipeline](../walkthroughs/deploy-a-pipeline)
- [Understand the loaded data](../using-loaded-data/understanding-the-tables)
- [Explore the loaded data in Streamlit](../using-loaded-data/exploring-the-data)
- [Transform the data with SQL or python](../using-loaded-data/transforming-the-data)
- [Contribute a pipeline](https://github.com/dlt-hub/pipelines)

Here are some example projects:
- [Is DuckDB a database for ducks? Using DuckDB to explore the DuckDB open source community](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)
- [Using DuckDB to explore the Rasa open source community](https://colab.research.google.com/drive/1c9HrNwRi8H36ScSn47m3rDqwj5O0obMk?usp=sharing)
- MRR and churn calculations on Stripe data
- Please open a PR to add projects that use `dlt` here!
