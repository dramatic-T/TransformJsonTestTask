from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, substring_index, max

from schemas.github_organization_schema import GithubOrganizationSchema


class SparkJsonTransformer:

    def transform(self):
        spark = SparkSession.builder \
            .appName("PrsDataTransform") \
            .config("spark.master", "local") \
            .getOrCreate()
        pulls_df = spark.read.json("repositories/*", multiLine=True)

        transformed_df = spark.createDataFrame(self.__transform_dataframe(pulls_df).rdd, schema=GithubOrganizationSchema.schema)

        transformed_df.printSchema()

        transformed_df.write.mode("overwrite").parquet("github_data.parquet")

        spark.stop()

    @staticmethod
    def __transform_dataframe(dataframe_reader):
        return dataframe_reader.groupBy(
            substring_index(col("repo.full_name"), '/', 1).alias("Organization_Name"),
            col("repo.id").alias("repository_id"),
            col("repo.name").alias("repository_name"),
            col("repo.owner.login").alias("repository_owner")
        ).agg(
            count("*").alias("num_prs"),
            count(when(col("pull_requests.merged_at").isNotNull(), True)).alias("num_prs_merged"),
            max(col("pull_requests.merged_at")).alias("merged_at")
        ).withColumn(
            "is_compliant",
            (col("num_prs") == col("num_prs_merged")) & col("repository_owner").rlike("scytale")
        ).select(
            "Organization_Name",
            "repository_id",
            "repository_name",
            "repository_owner",
            "num_prs",
            "num_prs_merged",
            "merged_at",
            "is_compliant"
        )