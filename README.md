# AI-Driven Support Analytics: End-to-End OCI Pipeline
> **A Medallion Architecture Project leveraging Gemini 3 Flash and Oracle Autonomous Database 26ai.**

## 🚀 The Mission
Modern support teams are often overwhelmed by ticket volume, leading to missed SLAs and customer churn. This project demonstrates a production-ready pipeline that transforms raw, unorganized support tickets into **actionable business intelligence**. 

By utilizing Generative AI for sentiment and category enrichment, the system automatically identifies high-priority escalations before they become critical business risks.

---

## 🏗️ Architecture & Data Flow
The project follows a **Medallion Architecture** (Bronze → Silver → Gold) to ensure data integrity and scalability.

![Architecture Diagram](assets/architecture_diagram.jpg)

1.  **Bronze (Ingestion):** Raw support tickets are ingested via Python and stored as a structured foundation.
2.  **Silver (Enrichment):** Data is processed through a Python-based AI engine using the **Google-GenAI SDK (Gemini 3 Flash)** to extract sentiment scores and ticket categories.
3.  **Gold (Analytics):** Enriched data is loaded into **Oracle Autonomous Database**, where Virtual Columns calculate real-time SLAs and Escalation Risks.

---

## 🛠️ Tech Stack & Credentials
Built by a certified **OCI 2025 Generative AI Professional** and **Autonomous Database Professional**.

* **Cloud:** Oracle Cloud Infrastructure (OCI)
* **AI/LLM:** Gemini 3 Flash Preview (Primary) & TextBlob (Local NLP Fallback)
* **Database:** Oracle Autonomous Lakehouse 26ai
* **Language:** Python 3.12 (Pandas, OCI SDK, Google-GenAI SDK)
* **Visualization:** OCI Charts

---

## 🛡️ Engineering Highlights & Resiliency
This project goes beyond a simple script by addressing real-world cloud engineering challenges:

* **Graceful Degradation:** Implemented a **Local NLP Fallback (TextBlob)**. If the Gemini API hits a rate limit (HTTP 429) or is unavailable, the pipeline automatically switches to local processing to ensure zero downtime.
* **Database-Level Intelligence:** Utilized **Oracle Virtual Columns** to bake business logic directly into the schema. This ensures that "Severity" and "Escalation Risk" are calculated consistently across all reporting tools.
* **Security First:** Leveraged **OCI IAM Authentication** (API Key-based) and **Network ACLs** to secure the database connection without exposing sensitive credentials.

---

## 📊 Business Insights
The final output is a dynamic dashboard that identifies operational bottlenecks:

![Business Dashboard](assets/business_dashboard.jpg)

### **Key Findings:**
* **SLA Criticality:** 61% of tickets exceeded the 8-hour response window, highlighting the need for the AI prioritization logic implemented here.
* **Emotional Hotspots:** Technical and Account-related tickets show the lowest sentiment, suggesting a need for targeted documentation improvements.
* **Regional Strategy:** Identified high-risk volumes in the CA-TORONTO and PH-NCR regions for staffing optimization.

---

## 📂 Project Structure
* `/assets`: Project diagrams and dashboard screenshots.
* `/scripts`: Python ETL and AI Enrichment logic (includes rate-limit handling).
* `/sql`: DDL for table creation with Virtual Columns and reporting queries.
* `/data`: Sample datasets showcasing raw vs. enriched states.

---

## 🎓 Author
**Junie Rose** *Principal Technical Support Engineer | Data Enghineer* *Specializing in OCI Data Management & Generative AI Solutions.*
