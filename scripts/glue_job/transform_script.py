import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

import sys
sys.path.insert(0, '/tmp/glue-modules')

from transforms.shared_transformations.default_transform import *
from utils.user_utils.user_config import *


def default_transforms(input_path: str):
    """
    given functions, call the transformation functions
    """
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

    logger.info(f"Reading data from: {input_path}")
    df = spark.read.parquet(input_path)

    df = drop_rows_with_empty_ssn_or_acct(df)
    df = drop_duplicate(df)
    df = encode_gender(df)
    return df

def user_defined_transform_with_yaml(input_path: str, config_path: str):
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

    logger.info(f"Reading data from: {input_path}")
    df = spark.read.parquet(input_path)

    logger.info(f"Loading transform config from: {config_path}")
    # user_transform data type: list
    user_transform_specs = parse_configs(config_path)['transformSpecs']
    udf_registry = get_udf_registry(config_path)

    for transformation in user_transform_specs:
        columns = transformation['features']
        function_name = transformation['udfName']
        udf_function = udf_registry[function_name]

        if udf_function:
            for col in columns:
                if col not in df.columns:
                    logger.info(f"column {col} not found in DataFrame. Skipped")
                    continue
                df = df.withColumn(col, udf_function(df[col]))
    return df



# Glue entry point
if __name__ == "__main__":
    args = sys.argv
    # check if we will use user defined function
    if "--config_path" in args:
        resolved_args = getResolvedOptions(sys.argv, ['input_path', 'config_path', 'output_path'])
        df = user_defined_transform_with_yaml(resolved_args['input_path'], resolved_args['config_path'])
        df.write.mode("overwrite").parquet(resolved_args['output_path'])
    else:
        resolved_args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])
        df = default_transforms(resolved_args['input_path'])
        df.write.mode("overwrite").parquet(resolved_args['output_path'])
