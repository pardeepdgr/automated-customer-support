from pyspark.sql import SparkSession


def read_csv(spark_session: SparkSession, path: str):
    r"""To import multiple csv file in single load; pass csv directory name as path argument."""
    return spark_session.read.csv(path, header='true', inferSchema='true')


def read_tsv(spark_session: SparkSession, path: str):
    return spark_session.read.csv(path, sep=r'\t', header='true', inferSchema='true')


def read_json(spark_session: SparkSession, path: str):
    return spark_session.read.json(path)


def read_text(spark_session: SparkSession, path: str):
    return spark_session.read.text(path)
