from pyspark.sql import DataFrame


def process_batch(df: DataFrame, epoch_id: int):
    cleansed_tweets = process_tweet_batch(df)
    print(cleansed_tweets)


# TODO: what if tweets are so huge in number
def process_tweet_batch(tweet_batch: DataFrame):
    tweets = tweet_batch.collect()
    extracted_tweets = [tweet.__getattr__("value") for tweet in tweets]
    return extracted_tweets
