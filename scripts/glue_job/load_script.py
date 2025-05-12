import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

import sys
sys.path.insert(0, '/tmp/glue-modules')
from utils.user_utils.user_config import parse_configs

def load_data(input_path: str, output_path: str):
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

    logger.info(f"Loading transformed data from: {input_path}")
    df = spark.read.parquet(input_path)

    logger.info(f"Saving to final destination: {output_path}")
    df.write.mode("overwrite").parquet(output_path)

def load_user_data(input_path: str, config_path: str):
    # get user data info
    user_data_info = parse_configs(config_path)['dataSource']
    output_data_path = user_data_info['destinationLocation']
    output_data_type = user_data_info['targetDataType']

    # get spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

    logger.info(f"Loading transformed data from: {input_path}")
    df = spark.read.parquet(input_path)

    logger.info(f"Saving to final destination: {output_data_path}")
    if output_data_type.lower() == "csv":
        df.write.mode("overwrite").option("header", "true").csv(output_data_path)
    elif output_data_type.lower() == "parquet":
        df.write.mode("overwrite").parquet(output_data_path)
    else:
        raise TypeError("Only support output format to be csv or parquet.")


# Glue entry point
if __name__ == "__main__":
    args = sys.argv
    if "--config_path" in args:
        resolved_args = getResolvedOptions(sys.argv, ['input_path', 'config_path'])
    else:
        resolved_args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])
        load_data(resolved_args['input_path'], resolved_args['output_path'])
