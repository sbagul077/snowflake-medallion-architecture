
# Snowflake Medallion Data Pipeline (Bronze → Silver → Gold) with AWS S3

A production-ready, version-controlled repository for a Snowflake-based data pipeline that ingests raw data from **AWS S3** into **Snowflake (Bronze)**, refines and standardizes it in **Silver**, and publishes curated analytics in **Gold**. The project includes reproducible **Snowflake notebooks**, **SQL DDL/DML**, and **Python utilities** for orchestration.

> Maintainer: **Sanket Bagul, Jivisha Eratkar, Srenivas, Akshay Patel K A**  
> Location: Pune, India  
> Tech: Snowflake, AWS S3, Snowpipe, Python, Jupyter

---

## 🎯 Goals
- Reliable ingestion from S3 to Snowflake using Snowpipe/Stage COPY.
- Clear, testable transformations across Medallion layers.
- Secure secrets handling (no credentials in Git!).
- Reproducible notebooks for exploration and validation.

---

## 📁 Repo Structure
```
snowflake-medallion-project/
├── KPIs/
│   ├── GOLD_LAYER_READMISSION_KPI.ipynb
│   ├── SILVER_LAYER_READMISSION_KPI.ipynb
│   ├── ADE per 100 KPI gold_allergies
│   ├── ADE per 100 KPI hl7_silver
│   ├── imm_gold_kpi.sql
│   ├── imm_silver_kpi.sql
│   ├── KPI Post‑Discharge Follow‑up within 48 hours.txt
│   ├── KPI explanation Post‑Discharge Follow‑up within 48 hours.txt
│   ├── Medication Errors per 100 patients GOLD LAYER TABLES.txt
│   ├── Medication Errors per 100 patients Silver layer views.txt
│   └── text.txt
│
├── SQL/
│   ├── final database.sql
│   ├── RAW_AND_BRONZE_LAYERS_OF_CSV.sql
│   ├── CCDA_FINAL_ASSIGNMENT.sql
│   ├── CSV_PARSER.sql
│   ├── test.sql
│   │
│   ├── CCDA/
│   │   ├── ccda.zip
│   │   ├── ccdaparser.py
│   │   └── CCDA_PARSER Master.ipynb
│   │
│   ├── CSV/
│   │   ├── CSV.txt
│   │   └── csv_1.zip
│   │
│   ├── HL7/
│       ├── HL7.txt
│       ├── HL7_ADT_1_300.zip
│       ├── HL7_ORM_1_100.zip
│       ├── HL7_ORU_1_100.zip
│       └── hl7_raw
│
├── notebooks/
│   ├── CCDA_PARSER Master.ipynb
│   └── Post Discharge Follow up within 48 hours of the discharge notification.ipynb
│
├── README.md
└── requirements.txt
```

## 🔐 Security & Secrets

# Snowflake
SNOWFLAKE_ACCOUNT=xxxxxxxxx
SNOWFLAKE_USER=your_user
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MEDALLION_DB
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_key.pem
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase

# AWS
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=my-bucket
S3_PREFIX=landing/



## 🚀 Quick Start

### 1) Clone & set up
bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp config/sample.env .env   # Fill in your local secrets


### 2) Create Snowflake objects
Run the DDL scripts in sql/ using Snowflake Web UI or a client:
sql
-- 00_init_database.sql
CREATE DATABASE IF NOT EXISTS MEDALLION_DB;
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.GOLD;


### 3) Configure Stage & Snowpipe (S3 → Snowflake)
Use snowflake_client.py or execute scripts that:
- Create an **external stage** pointing to your S3 bucket.
- Define a **file format** (e.g., JSON/CSV/Parquet).
- Create **Snowpipe** with auto-ingest from S3 events (or run COPY INTO).

Example SQL (simplified):
sql
CREATE OR REPLACE FILE FORMAT ff_json TYPE = JSON;
CREATE OR REPLACE STAGE s3_stage
  URL='s3://<bucket>/<prefix>'
  CREDENTIALS=(AWS_KEY_ID='...' AWS_SECRET_KEY='...')
  FILE_FORMAT=ff_json;

#XML File Format
CREATE OR REPLACE FILE FORMAT FF_XML
  TYPE = XML
  DISABLE_AUTO_CONVERT = TRUE;

#JSON File Format
CREATE OR REPLACE FILE FORMAT HL7_BRONZE_FF
  TYPE = 'CSV'
  FIELD_DELIMITER = '\t'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  NULL_IF = ('', 'NULL')
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE;

#CSV File Format
CREATE OR REPLACE FILE FORMAT FF_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('NULL');
 
CREATE OR REPLACE FILE FORMAT FF_RAW
  TYPE = CSV
  FIELD_DELIMITER = '\t'       -- entire line goes into one column
  SKIP_HEADER = 0              -- keep header if you want it
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE;
---

## 🧱 Medallion Layers
- **Bronze**: Raw, immutable ingestion from S3; minimal schema enforcement.
- **Silver**: Cleaned/standardized data; deduped, typed columns, basic QA.
- **Gold**: Business-ready marts; dimensional models, aggregates, dashboards.

---

## 🔗 Snowflake & S3 Connections (Python)
Basic connectors used in src/:
python
# snowflake_client.py
import os
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    private_key=open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), 'rb').read()
)


## 🧪 Testing
- Use pytest for Python modules in src/.
- Add data quality checks for Silver (nulls, types, duplicates).
- Include sample test data in /tests/data/ (non-sensitive).

---

## 📦 Requirements
Minimal requirements.txt:

snowflake-connector-python
boto3
python-dotenv
pandas
snowflake
pytest


---

## 🛡️ Best Practices
- Keep **DDL/DML** versioned under sql/.
- Treat notebooks as **reproducible experiments**; production logic belongs in src/.
- Use **roles/least privilege** in Snowflake and **IAM roles** in AWS.
- Prefer **external stages** + **Snowpipe** for scalable ingestion; 
  fall back to scheduled COPY INTO for simpler setups.
- Store PII securely; consider masking policies and row access policies.

---

## 📚 References
- Snowflake Stages & Snowpipe: https://docs.snowflake.com/en/user-guide/data-load-s3
- Snowflake Security & Access Control: https://docs.snowflake.com/en/user-guide/security-access-control
- AWS S3 & IAM: https://docs.aws.amazon.com/IAM/latest/UserGuide/access.html

---
---

## 🙋 Support
Issues and PRs are welcome. For questions, open an issue or reach out via GitHub.
