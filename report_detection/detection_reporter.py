from pyspark.sql import functions, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


def create_spark_session_with_s3():
    return SparkSession \
        .builder \
        .appName("DetectionReporter") \
        .master("local") \
        .config("fs.s3a.access.key", "") \
        .config("fs.s3a.secret.key", "") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.2") \
        .getOrCreate()


def read_hive_struct_type():
    window_spec = Window.partitionBy("country").orderBy("detection")
    return df.select(
        functions.col("metadata").getItem("country").alias("country"),
        functions.col("detection").getItem("name").alias("detection")
    ) \
        .withColumn("rank", rank().over(window_spec)) \
        .filter(col("rank") <= 1)


if __name__ == "__main__":
    spark = create_spark_session_with_s3()

    df = spark.read.json("s3a://nor-lo-test/test-table/")
    cleansed_df = read_hive_struct_type()
