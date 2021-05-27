from pyspark.sql import DataFrame


def save(message: DataFrame):
    message.write \
        .format("mongo") \
        .mode("append") \
        .option("database", "ultimate_ai") \
        .option("collection", "messages") \
        .save()
    pass
