from pyspark.sql import DataFrame


def save(message: DataFrame):
    message.write \
        .format("mongo") \
        .mode("append") \
        .save()
    pass
