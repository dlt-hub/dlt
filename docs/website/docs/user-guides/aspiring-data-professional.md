
# The capstone project that proves your pragmatic skills.

As senior data professionals ourselves, we have hired for many companies in the past and know what they look for.

Here we suggest some use cases for dlt that will enable aspiring or junior data professionals to showcase their understanding of a “valuable project”.


### Giving data to decision-makers, or automating decisions

Fundamentally there are 2 types of data problems:

- Those facing operational users, requiring development of general business analytics capabilities for the company, such as data warehouses, dashboards, self service.
- And those automating decisions or generating insights through the use of ML, who work on development of data products that solve specific business problems.

## 1) The business analytics path capstone project - a chance to show you can deliver results, not numbers.

The goal of such a project is to get you in the top 5% of candidates that gets you invited to an interview, and once at the interview understand pragmatically what is expected of you.

Depending on whether you want to be more in engineering or analytics, you can focus on different parts of this project. If you showcase that you are able to deliver end to end, there remains little reason for a potential employer to say no.

Someone hiring for the business analytics path will be looking for the following skills

- Can you load data to a db? Here the skills we are looking for are
    - can you do incremental loading?
    - are your pipelines maintainable?
    - are your pipelines reusable, do they take meaningful arguments?
- Can you transform the data to a standard architecture?
    - do you know dimensional modelling architecture, which enables business stakeholders to access data via pivot-table like interfaces?
    - do you model the data to be accessible via a user facing tool to a business user?
    - can you translate a business requirement into a technical one?
- Can you identify a use case and prepare reporting?
    - are you displaying a sensible use case?
    - are you taking a pragmatic approach to what should be displayed and why?
    - did you hard code charts in a notebook that the end user cannot use? or did you use a user-facing dashboard tool?
    - is the user able to answer follow up questions by changing the dimensions in a tool, or did you hard code queries?

So a project you could do would be:

1. Choose an api that produces data. If this data is somehow business relevant, that’s better. Many business apps offer free developer accounts that allow you to develop business apps with them.
2. Choose a use case for this data. Make sure this use case makes some business sense and is not completely theoretical. Business understanding and pragmatism are key for such roles, so do not waste your chance to show it. Keep the case simple - otherwise it will not be pragmatic right off the bat, handicapping yourself from a good outcome.
3. Build a dlt pipeline that covers this use case. Show them what you’ve got, keep the case simple, but do solid engineering. Use incremental loading, don’t hardcode parameters subject to change.
4. Build a dbt package for this pipeline. Docs here: [run dbt from local or repository](../using-loaded-data/transforming-the-data)
5. Build a visualisation. Focus on usability more than code. Remember, your goal is to empower a business user to self serve, so hard coded dashboards are usually seen as liabilities that need to be maintained. On the other hand, dashboard tools can be adjusted by business users too. For example the free Google’s “Looker studio” is relatable to business users, while notebooks might make them feel insecure. Your evaluator will likely not take time to set up and run your things, so make sure your outcomes are well documented with images. Make sure they are self readable, explain how you intend the business user to use this visualisation to fulfil the use case.
6. Make it presentable somewhere public such as github, add docs. Show it to someone for feedback. You will find likeminded people in our slack channel that will happily give their opinion.

## 2) The ML/data products path

Here the challenges might seem different, but in fact are very similar. Many courses focus on statistics and data science, machine learning, python, data ops, but very few focus on pragmatic approaches to business problems in organisations. Most of the times, the obstacle to a ML problem is not purely algorithmic but rather about semantics of the business, the data, or the human element that needs to cooperate to achieve usage of data products.

So what is a good project here?

Employers look for a project like above, that showcases both technical ability and business pragmatism in a use case. Remember, your competition here is quite high so you have to have high standards. Show that you are good at both things that may be expected of you.

And remember, in reality data does not come in files from the year 2000, but in apis with fresh data, where you usually have to grab it and move it somewhere to use. So show your ability to deliver end to end.

A good project might be something like

1. Choose an api that produces data. If this data is somehow business relevant, that’s better. Many business apps offer free developer accounts that allow you to develop business apps with them.
2. Choose a use case for this data. Make sure this use case makes some business sense and is not completely theoretical. Business understanding and pragmatism are key for such roles, so do not waste your chance to show it. Keep the case simple - otherwise it will not be pragmatic right off the bat, handicapping yourself from a good outcome. A few examples would be: Ranking leads in a sales CRM. Or clustering users, or maybe something about customer lifetime value predictions.
3. Build a dlt pipeline that covers this use case. Show them what you’ve got, keep the case simple, and your code clean. Use explicit variable and method names, tell a story with your code. For loading mode, use incremental loading, don’t hardcode parameters subject to change.
4. Build a dataset with sql. If you are ambitious you could try running the sql with a dbt package.  Docs here: [run dbt from local or repository](../using-loaded-data/transforming-the-data)
5. Showcase your chosen use case that uses ml or statistics to achieve your goal. Don’t forget to mention how you plan about applying the learning “in production”. Choose a case that is simple so you don’t end up overcomplicating your solution. Focus on outcomes and next steps. Describe what the company needs to do to use your results, to demonstrate that you understand the costs of your propositions.
6. Make it presentable somewhere public such as github, add docs. Show it to someone for feedback. You will find likeminded people in our slack channel that will happily give their opinion.

Do you have example projects you want to show off? Do a PR and link to your github project.

Examples:
- We showcase dlt in this [colab duckdb](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing) If you are a data scientist applying for a company that has pubic repos, you could do a spin off of this.
- We are planning to add other examples such as mrr and churn calculations on stripe data.
- Do you have examples that involve dlt? do a pull request to our [docs](https://github.com/dlt-hub/dlt/tree/devel/docs/website/docs) with your example.
