# AI-Driven Support Analytics: End-to-End OCI Pipeline
> **An OCI-native Medallion Architecture project leveraging Apache Airflow, Gemini 3 Flash, OCI Object Storage, and Oracle Autonomous Database 26ai.**

## 🚀 The Mission
Modern support teams are often overwhelmed by ticket volume, leading to missed SLAs and customer churn. This project demonstrates a production-ready pipeline that transforms raw, unorganized support tickets into **actionable business intelligence**. 

By utilizing Generative AI for sentiment and category enrichment, the system automatically identifies high-priority escalations before they become critical business risks.

---

## 🏗️ Architecture & Data Flow
The project follows a **Medallion Architecture** pattern: Bronze → Silver → Gold

![Architecture Diagram](assets/architecture_diagram.jpg)

1. **Bronze Layer (Raw):** Raw support tickets are ingested via Python and stored as a structured CSV files.
2. **Silver Layer (AI Enrichment):** Data is processed through a Python-based AI engine using the **Google-GenAI SDK (Gemini 3 Flash)** to extract sentiment scores and ticket categories. 
3. **Gold Layer (Analytics):** Enriched data is loaded into **Oracle Autonomous Database**, where Virtual Columns calculate real-time SLAs and Escalation Risks.

---

## 🌬️ Airflow Orchestration

The pipeline is orchestrated through Apache Airflow, providing workflow scheduling, dependency management, and monitoring.

![Airflow DAG](assets/airflow_dag_success.jpg)

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

## 🛡️ Engineering Highlights & Resiliency
This project goes beyond a simple script by addressing real-world cloud engineering challenges:

* **Graceful Degradation:** Implemented a **Local NLP Fallback (TextBlob)**. If the Gemini API hits a rate limit (HTTP 429) or is unavailable, the pipeline automatically switches to local processing to ensure zero downtime.
* **Database-Level Intelligence:** Utilized **Oracle Virtual Columns** to bake business logic directly into the schema. This ensures that "Severity" and "Escalation Risk" are calculated consistently across all reporting tools.
* **Security First:** Leveraged **OCI IAM Authentication** (API Key-based) and **Network ACLs** to secure the database connection without exposing sensitive credentials.
* **Cross-Platform Engineering:** During development, the pipeline was maintained simultaneously in Windows and Ubuntu (WSL) environments. Resolving filesystem path differences, Python environment issues, and Git synchronization challenges provided valuable experience in managing real-world development workflows.
---

## 📊 Business Insights
The final output is a dynamic dashboard that identifies operational bottlenecks:

![Business Dashboard](assets/business_dashboard.jpg)

### **Key Findings:**
* **Unresolved Ticket Risk:** Approximately 20% of support tickets remain unanswered, exposing potential SLA breaches and customer satisfaction risks. The pipeline surfaces these tickets early for proactive intervention.
* **Customer Experience Hotspots:** AI-driven sentiment analysis identified **Technical** and **Billing** issues as the primary sources of negative customer sentiment, enabling support teams to focus improvement efforts on the areas with the greatest customer impact.
* **Operational Resiliency:** The enrichment process successfully maintained analytics coverage through a combination of **Gemini AI** and **local NLP fallback processing**, demonstrating a fault-tolerant architecture capable of delivering business insights even during external AI service disruptions.

---

## 📂 Project Structure
* `/dags`: Apache Airflow orchestration workflows.
* `/src/pipelines`: Core pipeline stages for ingestion, enrichment, and loading.
* `/src/utils`: Shared OCI Object Storage utilities.
* `/assets`: Project diagrams and dashboard screenshots.
* `/sql`: DDL for table creation with Virtual Columns and reporting queries.
* `/data`: Local raw, temporary, and enriched datasets used during development.

---

## 🎓 Author
**Junie Rose** *Principal Technical Support Engineer | Data Engineer* *Specializing in OCI Data Management & Generative AI Solutions.*
