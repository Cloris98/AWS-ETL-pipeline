from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from airflow.utils.task_group import TaskGroup

from utils.deploy_utils.trigger_glue_job import start_glue_job, create_glue_job
from utils.glue_helper.glue_utils import get_date_stamp, zip_folder, upload_to_s3

default_args = {
    'owner': 'jingwei',
    'start_date': datetime(2025,4,30),
    'retries': 0
}
timestamp = get_date_stamp()
with DAG(
    dag_id='user_defined_etl_pipeline',
    default_args=default_args,
    description='etl pipeline with yaml file assigned requirement',
    schedule=None,
    catchup=False,
    tags=['etl', 'AWS-Glue', "udf"]
) as dag:
    # common values
    # common values
    # =============== USER CONFIGURATION SECTION =============
    # change the following values with your own aws resource names

    # s3 bucket to store Glue scripts and zipped dependencies
    script_bucket = "your-glue-script-bucket"

    # folder prefix in you script bucket, optional
    script_prefix = "scripts"

    # IAM role with AWS Glue, S3 permission
    iam_role = "your_iam_role"

    # the bucket your glue can temporarily write in it
    temp_dir = f"s3://{script_bucket}/temp/"

    # Optional: your raw data, transformed data, and output data bucket
    raw_data_bucket = "your-rawdata-bucket"

    # For Yaml-based runs
    user_yaml_file = "your_yaml_file"
    user_yaml_bucket = "your-yaml-config-bucket"
    user_prefix = "your-prefix"

    # udf information: udf folder name, udf zipped file name
    udf_folder = "your_udf_folder"
    udf_zipped_file = "your_udf_zipped_file"
    # ============================================

    # zip the udf folder
    zip_udf_utils = PythonOperator(
        task_id='zip_udf',
        python_callable=zip_folder,
        op_args=[
            f'/opt/airflow/udf/{udf_folder}',  # source folder path
            f'/opt/airflow/zipped_scripts/{udf_zipped_file}.zip'
        ]
    )

    with TaskGroup("upload_zipped_files") as upload_zipped_files:

        # upload zipped utils to s3 bucket
        upload_zipped_udf_to_s3 = PythonOperator(
            task_id='upload_zipped_udf_to_s3',
            python_callable=upload_to_s3,
            op_args=[
                f'/opt/airflow/zipped_scripts/{udf_zipped_file}.zip',
                f's3://{script_bucket}/{script_prefix}/{udf_zipped_file}.zip'
            ]
        )

        # upload zipped transform functions to s3 bucket
        upload_user_yaml_to_s3 = PythonOperator(
            task_id='upload_yaml_file_to_s3',
            python_callable=upload_to_s3,
            op_args=[
                f'/opt/airflow/user_config/{user_yaml_file}.yaml',  # change this once you upload your yaml file
                f's3://{user_yaml_bucket}/{user_prefix}/{user_yaml_file}.yaml' # change this once you upload your yaml file
            ]
        )
        upload_zipped_utils_to_s3 = PythonOperator(
            task_id='upload_zipped_utils_to_s3',
            python_callable=upload_to_s3,
            op_args=[
                '/opt/airflow/zipped_scripts/utils.zip',
                f's3://{script_bucket}/{script_prefix}/utils.zip'
            ]
        )

        upload_zipped_udf_to_s3 >> upload_user_yaml_to_s3 >> upload_zipped_utils_to_s3

    create_user_transform_job = PythonOperator(
        task_id="create_user_transform_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "user_transform_job",
            "script_location": f"s3://{script_bucket}/{script_prefix}/transform_script.py",
            "iam_role": iam_role,
            "temp_dir": temp_dir,
            "extra_files": [
                f"s3://{script_bucket}/{script_prefix}/utils.zip",
                f"s3://{script_bucket}/{script_prefix}/transforms.zip",
                f"s3://{script_bucket}/{script_prefix}/{udf_zipped_file}.zip"
            ]
        }
    )

    # run aws glue jobs (change the arguments to run with user uploaded yaml):
    with TaskGroup("ETL") as etl_process:
        # extract
        run_extract = PythonOperator(
            task_id="run_extract",
            python_callable=start_glue_job,
            op_kwargs={
                "job_name": "extract_job",
                "arguments": {
                    # make sure this same as the location that you upload your yaml file
                    "--config_path": f"s3://{user_yaml_bucket}/{user_prefix}/{user_yaml_file}.yaml",
                    "--output_path": f"s3://{raw_data_bucket}/stage/extracted/{user_prefix}"
                }
            }
        )

        # transform
        run_user_transform = PythonOperator(
            task_id="run_user_transform",
            python_callable=start_glue_job,
            op_kwargs={
                "job_name": "user_transform_job",
                "arguments": {
                    "--input_path": f"s3://{raw_data_bucket}/stage/extracted/{user_prefix}", # change as needed
                    "--config_path": f"s3://{user_yaml_bucket}/{user_prefix}/{user_yaml_file}.yaml",  # change as needed
                    "--output_path": f"s3://{raw_data_bucket}/stage/transformed/{user_prefix}/"
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
                    "--input_path": f"s3://{raw_data_bucket}/stage/transformed/{user_prefix}/", # same as above output for transform step
                    "--config_path": f"s3://{user_yaml_bucket}/{user_prefix}/{user_yaml_file}.yaml" # should same as your yaml file path
                }
            }
        )

        run_extract >> run_user_transform >> run_load
    zip_udf_utils >> upload_zipped_files >> create_user_transform_job >> etl_process

