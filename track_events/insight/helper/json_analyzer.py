from pyspark.sql import DataFrame, functions
from pyspark.sql.functions import col, lit


def users_set_price_filter_in_euro(data: DataFrame):
    return data \
        .select("*", functions.json_tuple("params", "event_content", "page").alias("event_content", "page")) \
        .select(col("session_id"), col("event_content")) \
        .filter(col("event_content") != lit("{}")) \
        .select("*", functions.json_tuple("event_content", "price", "currency").alias("price", "currency")) \
        .drop(col("event_content")) \
        .filter(col("currency") == lit("EUR"))


def users_set_price_filter_above(amount: int, data: DataFrame):
    return data.filter(col("price") > amount).groupBy("session_id").count().count()


def users_set_price_filter_below(amount: int, data: DataFrame):
    return data.filter(col("price") < amount).groupBy("session_id").count().count()


def user_session_contains_request_id(data: DataFrame):
    return data \
        .select("*", functions.json_tuple("params", "event_content", "page").alias("event_content", "page")) \
        .select(col("session_id"), col("page")) \
        .filter(col("page") != lit("{}")) \
        .select("*", functions.json_tuple("page", "request_id").alias("request_id")) \
        .drop(col("page")) \
        .filter(col("request_id") != lit("{}")) \
        .groupBy("session_id") \
        .count().count()
