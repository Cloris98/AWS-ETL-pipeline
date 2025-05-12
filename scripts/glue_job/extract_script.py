import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

import sys
sys.path.insert(0, '/tmp/glue-modules')
from utils.user_utils.user_config import parse_configs


def extract_data(input_path: str, output_path: str):
    """
    extract raw data without yaml file
    """
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

    logger.info(f"Extracting from: {input_path}")
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [input_path], "recurse": True},
        format_options={"withHeader": True}
    )

    logger.info(f"Writing extracted data to: {output_path}")
    datasource.toDF().write.mode("overwrite").parquet(output_path)

def extract_user_data(config_path: str, output_path: str):
    """
    extract user uploaded data
    """
    # get user data info
    user_data_info = parse_configs(config_path)['dataSource']
    input_data_path = user_data_info['sourceLocation']
    input_data_type = user_data_info['sourceDataType']

    # get spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

    # check the input data type to use the correct way to extract data
    logger.info(f"Extracting from: {input_data_path}")
    if input_data_type.lower() == "csv":
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            format="csv",
            connection_options={"paths": [input_data_path], "recurse": True},
            format_options={"withHeader": True}
        )
    elif input_data_type.lower() == "parquet":
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            format="parquet",
            connection_options={"paths": [input_data_path], "recurse": True}
        )
    else:
        raise ValueError(f"Unsupported format: {input_data_type}")


    # check the output data type to overwrite the new data in correct format
    logger.info(f"Writing extracted data to: {output_path}")
    datasource.toDF().write.mode("overwrite").parquet(output_path)

# Glue entry point
if __name__ == "__main__":
    args = sys.argv
    # Check if user uploads a config YAML file
    if "--config_path" in args:
        resolved_args = getResolvedOptions(sys.argv, ['config_path', 'output_path'])
        extract_user_data(resolved_args['config_path'], resolved_args['output_path'])
    else:
        resolved_args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])
        extract_data(resolved_args['input_path'], resolved_args['output_path'])