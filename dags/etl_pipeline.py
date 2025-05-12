from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from airflow.utils.task_group import TaskGroup

from utils.deploy_utils.trigger_glue_job import start_glue_job, create_glue_job
from utils.glue_helper.glue_utils import get_date_stamp, zip_folder, upload_to_s3

default_args = {
    'owner': 'xxx', # you can change it
    'start_date': datetime(2025,4,30),
    'retries': 0
}
timestamp = get_date_stamp()

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='trigger AWS Glue to do ETL',
    schedule=None,
    catchup=False,
    tags=['etl', 'AWS-Glue']
) as dag:
    # common values
    # =============== USER CONFIGURATION SECTION =============
    # change the following values with your own aws resource names

    #s3 bucket to store Glue scripts and zipped dependencies
    script_bucket = "your-glue-script-bucket"

    # folder prefix in you script bucket, optional
    script_prefix = "scripts"

    # IAM role with AWS Glue, S3 permission
    iam_role = "your_iam_role"

    # the bucket your glue can temporarily write in it
    temp_dir = f"s3://{script_bucket}/temp/"

    # Optional: your raw data, transformed data, and output data bucket
    raw_data_bucket = "your-rawdata-bucket"
    data_process_bucket = "your-transformed-data-bucket"
    output_data_bucket = "your-output-data-bucket"
    #============================================

    with TaskGroup("zip_files") as zip_files:
        # zip the utils folder
        zip_utils = PythonOperator(
            task_id='zip_utils',
            python_callable=zip_folder,
            op_args=[
                '/opt/airflow/utils/',  # source folder path
                '/opt/airflow/zipped_scripts/utils.zip'
            ]
        )

        # zip the transform folder
        zip_transform = PythonOperator(
            task_id='zip_transform',
            python_callable=zip_folder,
            op_args=[
                '/opt/airflow/transforms/',  # source folder path
                '/opt/airflow/zipped_scripts/transforms.zip'
            ]
        )

        zip_utils >> zip_transform

    with TaskGroup("upload_zipped_files") as upload_zipped_files:

        # upload zipped utils to s3 bucket
        upload_zipped_utils_to_s3 = PythonOperator(
            task_id='upload_zipped_utils_to_s3',
            python_callable=upload_to_s3,
            op_args=[
                '/opt/airflow/zipped_scripts/utils.zip',
                f's3://{script_bucket}/{script_prefix}/utils.zip'
            ]
        )

        # upload zipped transform functions to s3 bucket
        upload_zipped_transforms_to_s3 = PythonOperator(
            task_id='upload_zipped_transforms_to_s3',
            python_callable=upload_to_s3,
            op_args=[
                '/opt/airflow/zipped_scripts/transforms.zip',
                f's3://{script_bucket}/{script_prefix}/transforms.zip'
            ]
        )

    with TaskGroup("upload_scripts") as upload_scripts:
        # upload the glue scripts to s3 bucket
        upload_extract_scripts_to_s3 = PythonOperator(
            task_id='upload_extract_script',
            python_callable=upload_to_s3,
            op_args=[
                '/opt/airflow/scripts/glue_job/extract_script.py',  # source folder path
                f's3://{script_bucket}/{script_prefix}/extract_script.py'
            ]
        )

        upload_transform_scripts_to_s3 = PythonOperator(
            task_id='upload_transform_script',
            python_callable=upload_to_s3,
            op_args=[
                '/opt/airflow/scripts/glue_job/transform_script.py',  # source folder path
                f's3://{script_bucket}/{script_prefix}/transform_script.py'
            ]
        )

        upload_load_scripts_to_s3 = PythonOperator(
            task_id='upload_load_script',
            python_callable=upload_to_s3,
            op_args=[
                '/opt/airflow/scripts/glue_job/load_script.py',  # source folder path
                f's3://{script_bucket}/{script_prefix}/load_script.py'
            ]
        )


    with TaskGroup("create_glue_jobs") as create_glue_jobs:
        # create AWS glue jobs
        create_extract_job = PythonOperator(
            task_id="create_extract_job",
            python_callable=create_glue_job,
            op_kwargs={
                "job_name": "extract_job",
                "script_location": f"s3://{script_bucket}/{script_prefix}/extract_script.py",
                "iam_role": iam_role,
                "temp_dir": temp_dir,
                "extra_files": [f"s3://{script_bucket}/{script_prefix}/utils.zip"]
            }
        )

        create_transform_job = PythonOperator(
            task_id="create_transform_job",
            python_callable=create_glue_job,
            op_kwargs={
                "job_name": "transform_job",
                "script_location": f"s3://{script_bucket}/{script_prefix}/transform_script.py",
                "iam_role": iam_role,
                "temp_dir": temp_dir,
                "extra_files": [
                    f"s3://{script_bucket}/{script_prefix}/utils.zip",
                    f"s3://{script_bucket}/{script_prefix}/transforms.zip",
                ]
            }
        )

        create_load_job = PythonOperator(
            task_id="create_load_job",
            python_callable=create_glue_job,
            op_kwargs={
                "job_name": "load_job",
                "script_location": f"s3://{script_bucket}/{script_prefix}/load_script.py",
                "iam_role": iam_role,
                "temp_dir": temp_dir,
                "extra_files": [f"s3://{script_bucket}/{script_prefix}/utils.zip"]
            }
        )

        create_extract_job >> create_transform_job >> create_load_job

    # run aws glue jobs (change the arguments to run with user uploaded yaml):
    with TaskGroup("ETL") as etl_process:
        # extract
        run_extract = PythonOperator(
            task_id="run_extract",
            python_callable=start_glue_job,
            op_kwargs={
                "job_name": "extract_job",
                "arguments": {
                    "--input_path": f"s3://{raw_data_bucket}/your_raw_data.csv",
                    "--output_path": f"s3://{data_process_bucket}/stage/extracted/"
                }
            }
        )

        # transform
        run_transform = PythonOperator(
            task_id="run_transform",
            python_callable=start_glue_job,
            op_kwargs={
                "job_name": "transform_job",
                "arguments": {
                    "--input_path": f"s3://{data_process_bucket}/stage/extracted/",
                    "--output_path": f"s3://{data_process_bucket}/stage/transformed/"
                }
            }
        )

        # load
        run_load = PythonOperator(
            task_id="run_load",
            python_callable=start_glue_job,
            op_kwargs={
                "job_name": "load_job",
                "arguments": {
                    "--input_path": f"s3://{data_process_bucket}/stage/transformed/",
                    "--output_path": f"s3://{output_data_bucket}/{timestamp}"
                }
            }
        )

        run_extract >> run_transform >> run_load


    zip_files >> upload_zipped_files >> upload_scripts >> create_glue_jobs >> etl_process
