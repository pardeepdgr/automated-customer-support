from pyspark.sql import SparkSession


def init_spark_session(app_name: str,
                       master="local"):
    return SparkSession \
        .builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()


def init_spark_session_with_mongo(app_name: str,
                                  master="local",
                                  mongo_ip="localhost:27017",
                                  binaries="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"):
    return SparkSession \
        .builder \
        .master(master) \
        .appName(app_name) \
        .config("spark.mongodb.input.uri", "mongodb://" + mongo_ip) \
        .config("spark.mongodb.output.uri", "mongodb://" + mongo_ip) \
        .config("spark.jars.packages", binaries) \
        .getOrCreate()


def init_spark_session_with_s3(app_name: str,
                               master="local",
                               s3_binaries="org.apache.hadoop.fs.s3a.S3AFileSystem",
                               hadoop_aws_binaries="org.apache.hadoop:hadoop-aws:3.0.2"):
    return SparkSession \
        .builder \
        .appName(app_name) \
        .master(master) \
        .config("fs.s3a.access.key", "<access_key_from_vault>") \
        .config("fs.s3a.secret.key", "<secret_key_from_vault>") \
        .config("fs.s3a.impl", s3_binaries) \
        .config("spark.jars.packages", hadoop_aws_binaries) \
        .getOrCreate()
