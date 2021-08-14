from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import collect_list, concat_ws, current_timestamp, regexp_replace
from pyspark.sql.streaming import StreamingQuery

from sink.mongodb_sink import MongoSink
from spark.session_init import init_spark_session_with_mongo


def init_stream():
    spark: SparkSession = init_spark_session_with_mongo("tweets_stream_processor")

    tweets: DataFrame = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 5555) \
        .load()

    query: StreamingQuery = tweets \
        .writeStream \
        .trigger(processingTime='20 seconds') \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()


def process_batch(batch: DataFrame, epoch_id: int):
    message: DataFrame = batch \
        .agg(concat_ws(", ", collect_list("value")).alias("tweets")) \
        .withColumn("content", regexp_replace("tweets", "http\S+|www.\S+|#|RT:", "")) \
        .select("content") \
        .withColumn("timestamp", current_timestamp())

    MongoSink("foo", "bar").save(message)


if __name__ == "__main__":
    init_stream()
