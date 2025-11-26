---
title: Run dlt in Snowflake
description: Run dlt in Snowflake Native App
keywords: [snowflake, native app, dlt]
---


dlt can run in or with Snowflake in a few different ways:

## Ways to run dlt

1. **Snowflake Native App**  
   Install the **Database Connector App** from the Snowflake Marketplace and run pipelines through the app UI.  

2. **Snowflake Notebook**  
   Run a dlt pipeline directly inside a Snowflake Notebook for interactive development and testing.  

3. **Snowpark Container Services (SPCS)**  
   Package dlt into a container and run it on SPCS for in-Snowflake execution.  

4. **External runner with Snowflake as destination**  
   Run dlt outside Snowflake (your infra or a third party) and load into Snowflake as the destination.  

## Choosing an option

- Use **Native App** for a packaged, repeatable experience.  
- Use **Notebook** for quick experiments.  
- Use **SPCS** for containerized runs inside Snowflake.  
- Use an **external runner** if your pipelines already run outside Snowflake.






