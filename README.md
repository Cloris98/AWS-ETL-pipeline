# Fully Cloud-Based ETL Pipeline with Airflow and AWS Glue

This project demonstrates a modular and fully cloud-based ETL pipeline using **Apache Airflow (in Docker)** to orchestrate **AWS Glue** jobs. The ETL logic is fully delegated to cloud services, enabling scalable, serverless data processing with minimal local setup.

---

## Project Structure

```
├── config/                            # Configuration and metadata files
├── dags/
│   ├── etl_pipeline.py                # Standard ETL pipeline
│   └── user_defined_etl_pipeline.py   # ETL with user-provided YAML and UDFs
├── logs/                              # Airflow logs (ignored by Git)
├── scripts/
│   └── glue_job/                      # Glue ETL scripts: extract, transform, load
├── templates/                         # Onboarding YAML config template
│   └── user_onboard_config.yaml
├── transforms/                        # Reusable transformation functions
│   └── shared_transformations/
│       ├── __init__.py
│       └── default_transform.py
├── udf/                               # User-defined transformation functions
├── user_yaml/                         # YAML configs from external users
├── utils/
│   ├── deploy_utils/
│   │   └── trigger_glue_job.py        # Glue job trigger logic
│   ├── glue_helper/
│   │   └── glue_utils.py              # Uploading/zipping helpers
│   └── user_utils/
│       └── user_config.py             # User YAML processing logic
├── zipped_scripts/                    # Locally zipped files to upload to S3
├── .env                               # Sample template for credentials
├── docker-compose.yaml                # Airflow deployment via Docker
├── Dockerfile                         # Image config (if customized)
└── requirements.txt                   # Required packages
```

---

## 🚀 Features

* Orchestration via Airflow DAGs running in Docker
* AWS Glue jobs for extraction, transformation, and loading
* Supports modular UDF-based transformation pipelines
* Parameterized YAML support for dynamic, user-defined ETL
* Modular folder structure for scalability and reuse

---

## ⚙️ Setup Instructions

### 1. Provide AWS Credentials

Create a `.env` file at the root of the project:

```env
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
```

> 🔐 Use `.env` as a guide.
### 2. Customize User Configuration (Required!)

In both `dags/etl_pipeline.py` and `dags/user_defined_etl_pipeline.py`, there is a clearly marked `USER CONFIGURATION SECTION`. You **must** replace the placeholder values:

#### For `etl_pipeline.py`
```python
script_bucket = "your-glue-script-bucket"
script_prefix = "scripts"
iam_role = "your_iam_role"
temp_dir = f"s3://{script_bucket}/temp/"
raw_data_bucket = "your-rawdata-bucket"
data_process_bucket = "your-transformed-data-bucket"
output_data_bucket = "your-output-data-bucket"
```
#### For `user_defined_etl_pipeline.py`
```python
script_bucket = "your-glue-script-bucket"
script_prefix = "scripts"
iam_role = "your_iam_role"
temp_dir = f"s3://{script_bucket}/temp/"
raw_data_bucket = "your-rawdata-bucket"
user_yaml_file = "your_yaml_file"
user_yaml_bucket = "your-yaml-config-bucket"
user_prefix = "your-prefix"
udf_folder = "your_udf_folder"
udf_zipped_file = "your_udf_zipped_file"
```
> 📝 All hardcoded AWS S3 paths and IAM roles have been replaced by variables.
> These variables must be set to match your AWS environment before execution.

### 3. Install Python Dependencies (if needed)

Inside your Docker container or Airflow environment, make sure you install:

```txt
boto3
python-dotenv
```

---

## 📡 Triggering Glue Jobs

All Glue jobs (extract, transform, load) are executed from Airflow via `boto3`. The job definitions and scripts are zipped and uploaded to S3 prior to triggering the jobs.

In `utils/deploy_utils/trigger_glue_job.py`, the placeholder path:

```python
"--additional-python-modules": "s3://your_glue_script_bucket/scripts/requirements.txt"
```

must be updated if you're using any custom dependencies in Glue.

---

## 📂 Data Flow Overview

1. Upload local scripts and zipped folders to `script_bucket`
2. Create Glue jobs via Airflow DAG
3. Trigger ETL flow in sequence: extract → transform → load
4. (Optional) YAML-based DAG allows UDF-driven transformations from external users

---

## 🛠️ Notes

* Make sure all target S3 buckets exist before running the pipeline
* IAM roles must have appropriate permissions for Glue, S3, and logs
* You can create additional DAGs to modularize extract/transform/load phases

---

## Contact

For questions or collaboration, feel free to reach out via GitHub or email me at [jingweihe198@gmail.com](jingweihe198@gmail.com).

---