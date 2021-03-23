from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from ai_ultimate.processor.batch_processor import process_batch


if __name__ == "__main__":
    spark: SparkSession = SparkSession \
        .builder \
        .appName("AutomatedCustomerSupport") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/ultimate_ai.messages") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/ultimate_ai.messages") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .master("local") \
        .getOrCreate()

    tweets: DataFrame = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 5555) \
        .load()

    query: StreamingQuery = tweets\
        .writeStream \
        .trigger(processingTime='20 seconds') \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()
