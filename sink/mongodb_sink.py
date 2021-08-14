from pyspark.sql import DataFrame


class MongoSink:

    def __init__(self, db_name: str, collection_name: str):
        self.db_name = db_name
        self.collection_name = collection_name

    def save(self, message: DataFrame):
        message.write \
            .format("mongo") \
            .mode("append") \
            .option("database", self.db_name) \
            .option("collection", self.collection_name) \
            .save()
