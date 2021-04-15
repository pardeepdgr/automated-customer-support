from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from processor.batch_processor import process_batch


if __name__ == "__main__":
    # Create Spark session with mongodb configs
    spark: SparkSession = SparkSession \
        .builder \
        .appName("AutomatedCustomerSupport") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .master("local") \
        .getOrCreate()

    # Read tweet stream from 5555 port
    tweets: DataFrame = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 5555) \
        .load()

    # Create streaming query and process batch of tweets after every 20 secs.
    query: StreamingQuery = tweets\
        .writeStream \
        .trigger(processingTime='20 seconds') \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()
