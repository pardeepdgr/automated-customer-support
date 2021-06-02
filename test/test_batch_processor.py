import unittest
from pyspark.sql import SparkSession, DataFrame
from ult_a.processor.batch_processor import process_batch


class MyTestCase(unittest.TestCase):

    def test_batch_processing(self):
        spark: SparkSession = SparkSession \
            .builder \
            .appName("AutomatedCustomerSupport") \
            .master("local") \
            .getOrCreate()

        tweets: DataFrame = spark.read.csv("resources/tweets.csv")
        actual_cleansed_tweets: DataFrame = process_batch(tweets, 1)
        expected_cleansed_tweets: DataFrame = spark.read.csv("resources/cleansed_tweets.csv")

        self.assertEqual(actual_cleansed_tweets, expected_cleansed_tweets)


if __name__ == '__main__':
    unittest.main()
