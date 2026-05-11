+++
title = "Fayssal Zeggar"
description = "Data Engineer portfolio focused on Snowflake, Python, SQL, ETL/ELT pipelines, analytics enablement, and modern data platform projects."
date = 2026-05-11
draft = false
+++

# Fayssal Zeggar

## Data Engineer | Snowflake, Python, SQL, Data Pipelines

I am a bilingual Data Engineer based in Toronto, focused on building reliable data pipelines, cloud data solutions, and analytics-ready platforms.

My professional experience is centered on Snowflake-based data engineering in the pharmaceutical industry, where data quality, governance, traceability, and collaboration with technical and business stakeholders are critical. I design and maintain ETL/ELT pipelines, integrate data from enterprise and manufacturing systems, optimize SQL models, support analytics users, and help make complex process data easier to access and use.

Alongside my professional work, I build portfolio projects that demonstrate modern data engineering architecture end to end. My featured project is a complete Recipe Data Platform using Snowflake, Kafka, Airflow, dbt, Spark, OpenRouter, FastAPI, Streamlit, and Docker.

[View the Recipe Data Platform](/projects/recipe-data-platform/)

[Read the Technical Deep Dive](/projects/recipe-data-platform-technical/)

## What I Do

### Cloud Data Engineering

I build and maintain data pipelines that connect source systems to analytical platforms. My strongest professional experience is in Snowflake, SQL, Python, ETL/ELT workflows, and data integration across regulated enterprise environments.

At Sanofi, I work on Snowflake-based pipelines that integrate data from systems such as APRM, MES, SAP, and LIMS for global manufacturing contexts. These pipelines support analytics, AI, ML, process monitoring, and decision-making use cases.

### Analytics Enablement

I care about making data usable, not only moving it from one system to another.

My work includes restructuring data models, optimizing SQL, supporting Power BI reporting, and building tools that simplify access to Snowflake data tables for subject matter experts. One of these tools supports more than 60 SME users by making warehouse data easier to access from JMP.

### Data Governance and Reliability

Because my professional work is in a regulated pharmaceutical environment, I pay close attention to controlled access, traceability, maintainability, and reliable data delivery.

I have worked with Snowflake administration responsibilities in a team context, including roles, shared databases, governance workflows, and schema-change practices.

### Applied AI and Data Products

I am interested in using AI where it brings real structure to messy data.

In my Recipe Data Platform project, an LLM is used as an enrichment component inside a data pipeline. The model extracts language, vegetarian classification, cuisine style, and main ingredient from recipe descriptions. The output is validated before it becomes trusted Silver-layer data.

## Professional Experience

## Sanofi - Data Engineer

**Toronto, Ontario, Canada**  
**June 2024 - Present**

I work as a Data Engineer in a pharmaceutical and manufacturing data environment, building cloud data solutions that support analytics, AI, and ML initiatives.

Key responsibilities and contributions include:

- Designing and maintaining Snowflake-based ETL/ELT data pipelines.
- Integrating data from multiple source systems, including APRM, MES, SAP, and LIMS.
- Working with manufacturing/process datasets involving approximately 6,000 parameters per batch.
- Building server-side scripts to create Snowflake stages using REST APIs.
- Supporting Snowflake administration in a team context, including roles, common databases, and governance workflows.
- Leading the migration of around 30 Power BI tables and data sources to Snowflake.
- Restructuring data models and optimizing SQL for performance, maintainability, and governance.
- Building and maintaining a UI that simplifies access to Snowflake tables in JMP for more than 60 SME users.
- Building and enhancing Power BI dashboards for process, quality, and performance insights.
- Supporting data scientists, engineers, smart factory teams, process teams, and business stakeholders.
- Working with Dataiku for process modeling, optimization, and data manipulation when connectors are needed.
- Contributing to GitHub-based delivery workflows, including approver and writer responsibilities for ETL pipelines.
- Working with scheduled data pipelines using Argo on Kubernetes and YAML configuration updates.

This role has strengthened my ability to design data flows that are not only technically functional, but also reliable, governed, and useful for real operational users.

## Presage - R&D Project Manager / Signal Processing / AI / Medical Devices Intern

**Paris, France**  
**March 2023 - October 2023**

At Presage, I worked in an R&D environment combining biomedical signal processing, machine learning, deep learning, medical device applications, and cloud prototyping.

Contributions included:

- Developing and evaluating machine learning and deep learning models for biomedical signal processing.
- Exposing technical functionality through REST APIs.
- Deploying components on Azure for prototyping and testing.
- Using Azure Functions and warehouse-related components.
- Coordinating project tasks and communicating progress to stakeholders.
- Contributing to product and documentation deliverables.

This experience helped me connect AI/ML development with API exposure, cloud deployment, stakeholder communication, and product-oriented delivery.

## Earlier Engineering Experience

### SECOM Engineering - Engineering Intern

**Nantes, France**  
**June 2022 - September 2022**

I supported engineering projects in a multidisciplinary environment, contributed to technical analyses and documentation, and worked with cross-functional teams on industrial engineering topics.

### Nidaplast Composites - Engineering Intern, Study Office

**France**  
**June 2021 - July 2021**

I developed Visual Basic tools to automate quotation creation and support hydraulic technical studies. These tools improved speed and consistency for sales and engineering teams and generated illustrated PDF reports for different water-retention scenarios.

## Featured Project

## Recipe Data Platform

The Recipe Data Platform is an end-to-end personal portfolio project designed to showcase modern data engineering workflows.

It processes recipe metadata from social content, stores it in Snowflake, enriches it with an LLM, models it through Bronze/Silver/Gold layers, computes analytics with Spark, orchestrates jobs with Airflow, exposes data through FastAPI, and presents insights in a Streamlit dashboard.

### Architecture

```text
TikTok / CSV seed data
        |
        | batch ingestion or Kafka event
        v
Snowflake Bronze
        |
        | OpenRouter LLM enrichment
        v
Snowflake Silver
        |
        +--> dbt Gold serving views --> FastAPI + Streamlit
        |
        +--> PySpark analytics --> Gold analytics tables
        |
        v
Airflow orchestration + Docker Compose local platform
```

### Technologies Used

- Python
- Snowflake
- SQL
- Kafka
- Airflow
- dbt
- PySpark
- OpenRouter / LLM enrichment
- FastAPI
- Streamlit
- Docker Compose
- Nginx deployment on a Linux server

### What It Demonstrates

- End-to-end data platform design.
- Event-driven ingestion with Kafka.
- Snowflake Bronze/Silver/Gold modeling.
- LLM-based semantic enrichment with schema validation.
- dbt serving models for API and dashboard consumers.
- Spark analytics tables for batch-style aggregations.
- Airflow orchestration for ingestion, enrichment, dbt, and analytics jobs.
- FastAPI and Streamlit as user-facing data products.
- Docker-based deployment and server debugging.

[Project Overview](/projects/recipe-data-platform/)

[Technical Deep Dive](/projects/recipe-data-platform-technical/)

## Technical Skills

### Strong Professional Skills

- Snowflake
- SQL
- Python
- ETL / ELT pipelines
- Data ingestion and integration
- Batch processing
- Data governance
- Data quality and reliability
- REST APIs
- Power BI
- Dataiku
- JMP / JMP scripting context
- Git / GitHub
- GitHub Actions and CI/CD concepts
- Argo on Kubernetes scheduling exposure
- YAML configuration
- Azure Functions
- Agile / Scrum collaboration
- Technical documentation
- Stakeholder communication
- Relational data modeling
- Query tuning and performance optimization

### Validated Through Portfolio Project

- Kafka
- Airflow
- dbt
- PySpark
- Streamlit dashboarding
- FastAPI application serving
- Docker Compose multi-service deployment
- Nginx reverse proxy configuration
- LLM enrichment workflows

## Education

## IMT Atlantique

**Diplome d'ingenieur / Generalist Engineer**  
**AI & Biomedical Engineering - Master's Degree Equivalent**  
**2020 - 2023**

Relevant academic themes:

- machine learning;
- deep learning;
- data management;
- biomedical engineering;
- robotics, electronics, and sensors.

## Universidad de los Andes

**Exchange Semester - Bogota, Colombia**  
**January 2022 - June 2022**

Focus: electronic and life science engineering.

## Certifications

- **Industrial Biotechnology** - University of Manchester, December 2024
- **Dataiku Core Designer** - Dataiku, November 2024
- **Introduction to Modern Data Engineering with Snowflake** - Snowflake, November 2024

## Languages

- French: native
- English: fluent
- Spanish: fluent

## Contact

- Email: [fayssal.zeggar@laposte.net](mailto:fayssal.zeggar@laposte.net)
- LinkedIn: [linkedin.com/in/fzeggar](https://linkedin.com/in/fzeggar)
- Portfolio: [portfolio.fayssal-zeggar.com](https://portfolio.fayssal-zeggar.com)

