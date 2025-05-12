from datetime import datetime

import boto3
from pathlib import Path
import zipfile
import logging


def get_date_stamp():
    return datetime.utcnow().strftime('%m%d%y_%H%M%S')


def upload_to_s3(local_path, s3_path, s3_folder=''):
    s3 = boto3.client('s3')
    logger = logging.getLogger("airflow.task")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)

    if s3_folder != '':
        key = f"{key}/{s3_folder}"
        s3_path = f"s3://{bucket}/{key}"

    s3.upload_file(str(Path(local_path).resolve()), bucket, key)
    logger.info(f"Uploaded {local_path} to {s3_path}")


def zip_folder(source_folder, zip_path):
    source_path = Path(source_folder)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in source_path.rglob('*'):
            if file.is_file():
                zipf.write(file, arcname=file.relative_to(source_path.parent))
