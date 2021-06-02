from pyspark.sql import SparkSession


def read_tracking_extract():
    spark_session = SparkSession \
        .builder \
        .appName('homelike') \
        .master('local') \
        .getOrCreate()

    return spark_session.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .option('delimiter', r'\t') \
        .csv('resource/tracking_extract.csv')
