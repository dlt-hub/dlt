---
title: Run dlt in Snowflake
description: Run dlt in Snowflake Native App
keywords: [snowflake, native app, dlt]
---


You can run dlt within Snowflake AI Data Cloud in a few different ways.

## Ways to run dlt

1. **Snowflake Native App**  
   Install the **Database Connector App** from the Snowflake Marketplace and run pipelines through the build-in UI.  

2. **Snowflake Notebook**  
   Run a dlt pipeline directly inside a Snowflake Notebook for interactive development and testing.  

3. **Snowpark Container Services (SPCS)**  
   Package dlt into a container and run it on SPCS for in-Snowflake execution (available starting with Snowpark 1.43.0). 

4. **External runner with Snowflake as destination**  
    [Run dlt outside](../walkthroughs/deploy-a-pipeline) Snowflake (your infra or a third party) and load into Snowflake as the destination.  

## How to choose

- Use **Native App** for a packaged, repeatable experience.  
- Use **Notebook** for quick experiments.  
- Use **SPCS** for containerized runs inside Snowflake.  
- Use an **external runner** if your pipelines already run outside Snowflake.






