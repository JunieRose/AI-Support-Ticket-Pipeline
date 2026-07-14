# AI-Enriched Support Analytics Pipeline
> **Production-Inspired Data Engineering Pipeline with Apache Airflow, OCI, Gemini AI & Oracle Autonomous Database**

## 🚀 The Mission
Modern support teams are often overwhelmed by ticket volume, leading to missed SLAs and customer churn.

This project demonstrates a **production-inspired, metadata-driven data engineering pipeline** that transforms raw support tickets into analytics-ready datasets through automated data quality validation, AI enrichment, and dimensional modeling.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** and is orchestrated using **Apache Airflow** on **Oracle Cloud Infrastructure (OCI)**.

Beyond business analytics, the project also implements **operational metadata**, enabling execution tracking, audit logging, configuration management, and pipeline observability similar to enterprise ETL platforms.

---
## ✨ Key Features

- 🟧 Bronze → Silver → Gold Medallion Architecture
- ✅ Automated data quality validation with quarantine
- 🤖 AI-powered enrichment using Google Gemini
- 🔄 Automatic TextBlob fallback for resiliency
- 📊 Star schema data warehouse
- 📈 Metadata-driven pipeline monitoring
- 📝 JSON execution metrics for every stage
- ☁️ OCI Object Storage integration
- 🛠 Apache Airflow orchestration
- 🔍 End-to-end execution lineage

---

## 🏗️ Architecture & Data Flow
![Architecture Diagram](assets/architecture_diagram.jpg)

### Pipeline Stages

1. **Generate Raw Data (Bronze)** – Creates synthetic support ticket datasets and stores them in OCI Object Storage.
2. **Validate Bronze Data** – Applies data quality rules, quarantines invalid records, and produces a validated dataset.
3. **AI Enrichment (Silver)** – Enriches validated tickets using Google Gemini with automatic TextBlob fallback.
4. **Load to Lakehouse (Gold)** – Loads enriched data into Oracle Autonomous Database using a Star Schema for analytics.

---

## 🌬️ Airflow Orchestration

The pipeline is orchestrated through Apache Airflow, providing workflow scheduling, dependency management, and monitoring.

![Airflow DAG](assets/airflow_dag_success.jpg)

The DAG orchestrates four stages:

1. Generate Raw Data
2. Validate Bronze Data
3. AI Enrichment
4. Load to Lakehouse
---

## 🛠️ Tech Stack & Credentials
Built by a certified **OCI 2025 Generative AI Professional** and **Autonomous Database Professional**.

* **Orchestration:** Apache Airflow
* **AI/LLM:** Gemini 3 Flash Preview (Primary) & TextBlob (Local NLP Fallback)
* **Cloud:** Oracle Cloud Infrastructure (OCI)
* **Database:** Oracle Autonomous Lakehouse 26ai
* **Storage:** OCI Standard Object Storgae
* **Language:** Python 3.11 (Pandas, OCI SDK, Google-GenAI SDK)
* **Visualization:** OCI Charts
* **Development:** Git, GitHub, VS Code, WSL

---

## 📊 Business Insights
The final output is a dynamic dashboard that identifies operational bottlenecks:

![Business Dashboard](assets/business_dashboard.jpg)

---

## 📂 Project Structure
* `/dags`: Apache Airflow DAGs for orchestrating the end-to-end pipeline.
* `/src/pipelines`: Core pipeline stages for data generation, validation, AI enrichment, and loading.
* `/src/utils`: Shared utilities for OCI Object Storage, Oracle database access, and operational metadata.
* `/assets`: Architecture diagrams, database schema, and dashboard screenshots.
* `/sql`: Database DDL, reference tables, metadata tables, and reporting queries.
* `/data`: Local raw, temporary, and enriched datasets used during development and testing.

---

## 🎓 Author
**Junie Rose** *Principal Technical Support Engineer | Data Engineer* *Specializing in OCI Data Management & Generative AI Solutions.*
