File - EDA:
jobs.csv — 500 rows, 5 columns

candidates.json — 2001 candidate records

education.csv — 2000 rows

applications.csv — 5000 rows

workflow_events.jsonl — 16,769 event records

dimensional_model_erd.png — ERD for the star schema

Instructions.md

A few useful observations from the data:

jobs.csv has mixed date formats in posted_date, for example:

2025/07/25

2025.01.01

2024-11-28

jobs.department has missing values

candidates.json contains unicode names like José, Noël, so your loader should open files with utf-8

candidates.skills is an array, so you should store it as JSON text in raw ingestion

workflow_events.jsonl has old_status = null for initial Applied events, which is normal

The ERD is simpler than you might expect:

dim_job

dim_candidate

fct_applications

fct_workflow_events