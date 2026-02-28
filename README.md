# NHS TRUD Data Platform

An automated, cloud-native data pipeline and analytical dashboard for processing and visualising the NHS TRUD dm+d (Dictionary of Medicines and Devices) dataset.

## Architecture

This project strictly adheres to industry standards, decoupling the backend ETL pipeline from the frontend analytical presentation layer to ensure modularity, scalability, and testability.

### 1. ETL Pipeline (`/etl`)
A containerised Python application deployed as a **Google Cloud Run Job**.
- **Role:** Fetches the latest dm+d XML releases (Items 24 and 25) directly from the NHS TRUD API, extracts nested archives, and performs memory-efficient data parsing.
- **Transformation:** Performs the complex joining of SNOMED Virtual Medicinal Products (VMPs) to British National Formulary (BNF) concepts and Anatomical Therapeutic Chemical (ATC) classifications.
- **Outputs:** Engineers the raw XML into strongly typed, resilient artefacts (Parquet, DuckDB, and Google BigQuery) mounted to a persistent Google Cloud Storage bucket (`/mnt/gcs`).
- **Testing:** Python logic is verified via `pytest` located in `etl/tests`.

### 2. Shiny Dashboard (`/dashboard`)
An interactive R Shiny application deployed as a **Google Cloud Run Web Service**.
- **Role:** Connects directly to the mapped Parquet files persisted in the GCS bucket.
- **Analysis:** Provides real-time insights into prescribing coverage metrics, anatomical group allocations, and specifically focuses on Antimicrobial (J01) coverage gap analysis for precise surveillance.
- **Testing:** R logic and reactive transformations are thoroughly tested using `testthat` located in `dashboard/tests/testthat`.

## Automation & Deployment

The platform is strictly deployed via **GitHub Actions** CI/CD workflows, removing the need for manual server provisioning.
- **`.github/workflows/deploy-etl.yml`**: Triggers on modifications to the `etl/` directory or on a weekly CRON schedule (every Tuesday) to automatically ingest the new TRUD release. Builds the Docker image to Google Artifact Registry and updates the Cloud Run Job.
- **`.github/workflows/deploy-dashboard.yml`**: Triggers on modifications to the `dashboard/` directory. Builds the Shiny Server image and deploys it as a scalable Cloud Run Service.

## Deployment Setup Guide

To deploy this platform to your own GCP environment:

1. **GCP Project & Services:** Enable Cloud Run, Cloud Build, Artifact Registry, BigQuery, and Cloud Storage in your GCP Project.
2. **Storage Bucket:** Create a standard regional GCS bucket (e.g., `r-projects-test`) to serve as the shared persistent volume between the ETL job and Dashboard.
3. **IAM Service Account:** Provision a Service Account with the following required roles:
    - Artifact Registry Writer
    - Cloud Run Admin
    - Service Account User
    - Cloud Scheduler Admin
    - BigQuery Data Editor
    - BigQuery User
    - Storage Object Admin
4. **GitHub Secrets:** Add the Service Account JSON key as a repository secret named `GCP_CREDENTIALS`. Add your API credentials as `TRUD_API_KEY`.
5. **Configuration:** Update the environment variables (`PROJECT_ID`, `GCS_BUCKET`, `REGION`, `AR_REPO`) within both GitHub Action YAML files to match your active GCP resources exactly.

This pipeline leverages Cloud Run Gen2 execution environments with explicit Cloud Storage volume mounts to guarantee fast, stateless operations with secure data persistence.
