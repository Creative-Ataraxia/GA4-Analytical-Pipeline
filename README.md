## GA4-to-Lakehouse e-Commerce Analytics Pipeline

### Executive Summary
A local, containerized data pipeline that ingests Google Analytics 4 (GA4) export data, lands it in an open-format lake, transforms it with PySpark, validates data contract with dbt tests, and produce two core product insights: conversion-funnel efficiency and session-level retention.

### Objectives
* Business
  * Quantify drop-offs across the session → page_view → purchase funnel and measure next-day/next-week return visits.
  * Stakeholder demo will show actionable numbers; funnel stages & retention tables refresh daily.
* Engineering
  * Deliver a modular lakehouse stack that runs end-to-end on a single M5.xlarge instance (<4 vCPU, <16GB RAM).	
  * Pipeline freshness ≤ 15 min for one day of data; all dbt tests pass.
* Benchmarking
  * Airflow parity task that diffs row counts between Postgres tables and the dbt-ga4 counterparts; fail DAG if Δ > 0.5 %.


### Scope & Business Goals
* Conversion Funnel (session_start → page_view → purchase)
  * Uses: event counts & timestamps.
* Session-Level Retention (D1, D7 return of user_pseudo_id)	
  * Uses: user-pseudo-IDs; avoids PII.
* Traffic-Source Performance
  * Uses: traffic_source fields; no lookup table needed at start.
* Governance / Access / Full catalog
  * Roadmap: for later phase once the MVP proves out.

### Data Sources
	* Sample GA4 data from BigQuery public datasets (92 days)

### Data Models
| Table                     | Grain                 | Key Columns           | Purpose                                       |
| ------------------------* | --------------------* | --------------------* | --------------------------------------------* |
| **fact\_events**          | event                 | `event_id`            | Raw events after JSON flattening.             |
| **fact\_sessions**        | GA4 session           | `session_id`          | Sessionisation logic reproduced from dbt-ga4. |
| **dim\_user**             | user\_pseudo\_id      | `user_pseudo_id`      | Device & geo attrs at first touch.            |
| **dim\_page**             | page\_view            | `page_id`             | URL, hostname, content group.                 |
| **dim\_traffic\_source**  | session               | `session_id`          | source / medium / campaign.                   |
| **agg\_funnel\_daily**    | date × stage          | `event_date`, `stage` | Daily funnel counts.                          |
| **agg\_retention\_daily** | cohort\_date × day\_n | keys above            | Session-level retention matrix.               |

### Architecture (Local)
```ascii
┌────────────┐    land    ┌───────────┐  transform    ┌─────────┐  write ┌──────────┐
  External DB ────────────►  MinIO(S3) ──────────────►   Spark   ────────► Postgres │ 
└────────────┘  (bronze)  └───────────┘   (silver)    └─────────┘ (gold) └──────────┘
                                                                     
```
* Orchestration * Apache Airflow (docker-compose executor)
* Transformations * PySpark 3.5 scripts (Bronze→Silver→Gold)
* Validation & Docs * dbt-postgres (tests + lineage graph)
* Containerization * Docker Compose; no IaC/Terraform in local

### Stack
| Layer             | Tool                      | Version | Notes                                                             |
| ----------------* | ------------------------* | ------* | ----------------------------------------------------------------* |
| Storage – raw     | MinIO                     | latest  | S3-compatible object store.                                       |
| Processing        | Apache Spark              | 3.5     | Local standalone cluster.                                         |
| Serving           | Postgres                  | 16      | Columnar ext optional (pg\_partman / Citus).                      |
| Modelling / Tests | **dbt**                   | 1.8     | Adapter: `dbt-postgres`; installs `dbt-ga4` package as reference. |
| Orchestration     | Airflow                   | 2.9     | One DAG per layer.                                                |
| Dev tools         | Makefile, pre-commit, tox | –       | Ensures repeatable local runs.                                    |

### Implementation Phases
| Phase                       | Deliverable                                    | Target Date |
| --------------------------* | ---------------------------------------------* | ----------* |
| **0. Bootstrap**            | Repo, docker-compose up, sample data in MinIO. | +3 days     |
| **1. Bronze Ingest**        | Airflow DAG: load Parquet → MinIO bronze.      | +1 wk       |
| **2. Silver Transform**     | PySpark flatten & schema enforcement.          | +2 wk       |
| **3. Gold Models**          | Sessionisation; build dim/fact tables.         | +3 wk       |
| **4. Validation**           | dbt tests + parity checks vs. dbt-ga4.         | +4 wk       |
| **5. Insights**             | SQL queries + simple dashboard (Superset).     | +5 wk       |
| **6. Documentation & Demo** | `dbt docs`, README, demo video.                | +6 wk       |

### Success Metrics
* Latency: ≤ 15 min for one-day slice end-to-end.
* dbt test pass rate: 100 % on not_null/unique checks.
* Parity: Δ row_count ≤ 0.5 % per table vs. dbt-ga4 package.
* Resource footprint: Fits in a single M5.xlarge EC2 instance.

### Risk Register
| Risk                                 | Impact             | Mitigation                                                    |
| -----------------------------------* | -----------------* | ------------------------------------------------------------* |
| GA4 schema drift                     | Transform fail     | Use Spark’s `PERMISSIVE` mode; unit tests on schema.          |
| PII Redacted sample limits depth     | Low business value | Stick to session-level metrics; clearly document data limits. |
| Local hardware constraints           | Runtime failures   | Work on 30-day slices; scale up as needed.                    |
| Airflow instability                  | Delays             | Healthchecks + DAG import test in CI.                         |

### Roadmap
* Data Quality / Observability – Great Expectations suites, OpenLineage registration.
* CI/CD & IaC – GitHub Actions, Terraform modules for S3 + EKS deployment.
* Catalog / Schema Registry – DataHub or Glue + Iceberg table format.
* Security & Privacy – PII masking, row-level IAM, bucket encryption.
* Transform Shift to dbt-Spark – Replace PySpark scripts with dbt models to unify lineage.
* Real-time Ingestion – GA4 Realtime API → Kafka → Spark Structured Streaming.
* Advanced Analytics – ML features for churn prediction, anomaly detection.