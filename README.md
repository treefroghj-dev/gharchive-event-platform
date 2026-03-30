# GHArchive Events Platform

A data engineering project for ingesting GHArchive event data, storing raw files in Google Cloud Storage, loading data into BigQuery, and transforming it with dbt.

## Overview

This project is built to create an end-to-end analytics pipeline for GitHub public event data.

Work flow:

GHArchive → GCS → BigQuery → dbt

Airflow is used for orchestration, Docker is used for local development, and Terraform is used for infrastructure provisioning.

## Tech Stack

- Python
- Apache Airflow
- Docker / Docker Compose
- Google Cloud Storage
- BigQuery
- dbt
- Terraform

## Project Structure

```text
gharchive-events-platform/
├── airflow/
│   ├── dags/
│   ├── docker-compose.yaml
│   └── Dockerfile
├── gharchive_dbt/
├── src/
│   └── gharchive_events/
├── tests/
├── pyproject.toml
└── README.md

## Onboarding

### 1. Clone the repository

### 2.Update the values in .env.example with your own configuration, then copy it to .env:

```bash
cp .env.example .env
```

Make sure the following values are set correctly in .env.

### 3. Build the Airflow image

```bash
docker build -f airflow/Dockerfile -t gharchive-airflow:latest .
```

### 4. Start Airflow locally

```bash
cd airflow
docker compose up
```

### 4. Verify the DAGs

```bash
docker compose exec airflow-scheduler airflow dags list
```

