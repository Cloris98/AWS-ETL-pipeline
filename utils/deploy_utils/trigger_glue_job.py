import boto3
import time

import logging
logger = logging.getLogger("airflow.task")


def create_glue_job(job_name, script_location, iam_role, temp_dir,
                    extra_files=None,
                    region='us-east-1'):
    """
    job_name: the glue job name, will be used while calling start_glue_job
    script_location: where the glue script located in the s3 bucket
    iam_role: the aws iam_role with s3:*, glue:*, logs:* permission
    temp_dir: Tells AWS Glue where to write temporary files during job execution
    region: aws job region
    """
    glue = boto3.client("glue", region_name=region)

    default_args = {
        "--TempDir": temp_dir,
        "--job-language": "python",
        "--additional-python-modules": "s3://your_glue_script_bucket/scripts/requirements.txt", # change this to your own glue script s3 bucket
        "--python-modules-installer-option": "-r"
    }

    if extra_files:
        default_args["--extra-py-files"] = ",".join(extra_files)

    try:
        glue.create_job(
            Name=job_name,
            Role=iam_role,
            ExecutionProperty={
                'MaxConcurrentRuns': 123
            },
            Command={
                'Name': "glueetl",
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            DefaultArguments=default_args,
            MaxRetries=3,
            GlueVersion="5.0",  # or "4.0" if you're using Spark 3.3+
            NumberOfWorkers=2,
            WorkerType="G.1X"
        )
        logger.info(f"✅ Glue Job '{job_name}' created.")
    except glue.exceptions.AlreadyExistsException:
        logger.info(f"ℹ️ Glue job '{job_name}' already exists — skipped.")
    except Exception as e:
        logger.info(f"❌ Failed to create job '{job_name}': {e}")


def start_glue_job(job_name, arguments=None, region="us-east-1"):
    glue = boto3.client("glue", region_name=region)
    try:
        response = glue.start_job_run(JobName=job_name,
                                     Arguments=arguments or {})
        job_run_id = response['JobRunId']
        logger.info(f"Started Glue Job: {job_run_id}")
    except:
        logger.info(f"Glue job {job_name} not found ")
        raise

    while True:
        status = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        logger.info(f"🔄 Status of {job_name}: {status}")
        if status in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
            break
        time.sleep(10)

    if status == "SUCCEEDED":
        logger.info(f"✅ Glue job {job_name} completed successfully.")
    else:
        raise Exception(f"❌ Glue job {job_name} failed with status: {status}")

    return job_run_id



