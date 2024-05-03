from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType


class GithubOrganizationSchema:
    schema = StructType([
        StructField("Organization Name", StringType(), True),
        StructField("repository_id", IntegerType(), True),
        StructField("repository_name", StringType(), True),
        StructField("repository_owner", StringType(), True),
        StructField("num_prs", IntegerType(), True),
        StructField("num_prs_merged", IntegerType(), True),
        StructField("merged_at", StringType(), True),
        StructField("is_compliant", BooleanType(), True)
    ])