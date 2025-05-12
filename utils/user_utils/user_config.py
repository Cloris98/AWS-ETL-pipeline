import importlib
import io

import boto3
import yaml
from pathlib import Path
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def parse_configs(user_yaml_path):
    """
    Your yaml file must be uploaded in user_config
    so your user_yaml_path should be something like: '../user_config/xxx.yaml'

    This returns a dictionary with user defined requirement
    """

    s3 = boto3.client("s3")
    bucket, key = user_yaml_path.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    yaml_content = obj["Body"].read()
    return yaml.safe_load(io.BytesIO(yaml_content))


def return_type(udf_return_type: str, element_type=None, key_type=None, value_type=None):
    RETURN_TYPE_MAP = {
        "string": StringType(),
        "int": IntegerType(),
        "boolean": BooleanType(),
        "float": FloatType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType(),
        "struct": StructType(),
        "decimal": DecimalType(),
        "null": NullType()
    }
    if udf_return_type == "array" and element_type:
        return ArrayType(element_type)
    elif key_type and value_type and udf_return_type == "map":
        return MapType(key_type, value_type)
    else:
        return RETURN_TYPE_MAP[udf_return_type]

def get_udf_registry(user_yaml_path):
    udf_infos = parse_configs(user_yaml_path)['transformSpecs']
    registry = {}
    for udf_info in udf_infos:
        func_name = udf_info['udfName']
        file_name = udf_info['udfFile']
        module_path = f"{file_name}.{func_name}"
        module = importlib.import_module(module_path)
        func = getattr(module, func_name, None)

        if not callable(func):
            raise TypeError(f"The function {udf_info['udfName']} is not callable.")

        udf_return_type = return_type(udf_info['returnType'])
        registry[udf_info['udfName']] = udf(func, udf_return_type)
    return registry









