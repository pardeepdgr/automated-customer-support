from pyspark.sql import DataFrame


def save(message: DataFrame):
    print(message.show(truncate=False))
    pass
