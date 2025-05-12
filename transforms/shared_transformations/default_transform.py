from pyspark.sql.functions import col, trim, when


def drop_rows_with_empty_ssn_or_acct(df):
    """
    drop the rows that ssn or account number is empty
    """
    return df.filter(
        (col("acct_num").isNotNull()) & (trim(col("acct_num")) != "") &
        (col("ssn").isNotNull()) & (trim(col("ssn")) != "")
    )

def drop_duplicate(df):
    return df.dropDuplicates()

def encode_gender(df):
    """
    convert 'gender' column 'F' -> 1 and 'M' -> 0
    """
    return df.withColumn("gender", when(col("gender") == "F", 1).otherwise(0))

