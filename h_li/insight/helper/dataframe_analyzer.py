from typing import List

from pyspark import Row
from pyspark.sql import DataFrame, Window, WindowSpec
from pyspark.sql.functions import col, datediff, first, last, lit, max, min, row_number, to_date


def count_by_devices(data: DataFrame):
    return data \
        .groupBy("device_class") \
        .count().withColumnRenamed("count", "device_count")


def count_user_sessions_first_started_with_an_om_source(data: DataFrame):
    window: WindowSpec = Window.partitionBy(col("session_id")).orderBy(col("datetime"))

    return data \
        .select("session_id", "datetime", "om_source") \
        .na.fill({"om_source": "no_om_source"}) \
        .withColumn("row_number", row_number().over(window)) \
        .filter((col("row_number") == lit(1)) & (col("om_source") != lit("no_om_source"))) \
        .select("session_id") \
        .groupBy("session_id") \
        .count().count()


def user_sessions_duration_less_than(duration: int, data: DataFrame):
    rows: List[Row] = data \
        .groupBy('session_id') \
        .agg(datediff(min(to_date(col('datetime'))), max(to_date(col('datetime')))).alias('session_duration')) \
        .filter(col('session_duration') <= duration) \
        .drop("session_duration").collect()

    users: List[str] = [str(row.session_id) for row in rows]
    return users


def user_journeys_started_with(page: str, users: List[str], data: DataFrame):
    return data.filter((col("session_id").isin(users))) \
        .groupBy("session_id") \
        .agg((first("page_type") == lit(page)).alias("first_page")) \
        .filter(col("first_page") == lit("true")) \
        .count()


def user_journeys_ended_with(page: str, users: List[str], data: DataFrame):
    return data.filter((col("session_id").isin(users))) \
        .groupBy("session_id") \
        .agg((last("page_type") == lit(page)).alias("last_page")) \
        .filter(col("last_page") == lit("true")) \
        .count()


def user_journeys_started_on_search_page_later_visited_apartment_view(data: DataFrame):
    users = user_journey_started_from("search_page", data)

    return data.filter((col("session_id").isin(users)) & (col("page_type") == lit("apartment_view"))).count()


def user_journey_started_from(page, data):
    window: WindowSpec = Window.partitionBy(col("session_id")).orderBy(col("datetime"))

    rows: List[Row] = data \
        .select("session_id", "datetime", "page_type") \
        .withColumn("row_number", row_number().over(window)) \
        .filter((col("row_number") == lit(1)) & (col("page_type") == lit(page))) \
        .collect()

    users: List[str] = [str(row.session_id) for row in rows]

    return users


def users_clicked_book_now(data: DataFrame):
    return data \
        .filter(col('event_type') == lit('click_book-now')) \
        .groupBy('session_id') \
        .count().count()


def users_started_with_search_page_and_executed_request_draft_created(data: DataFrame):
    users = user_journey_started_from("search_page", data)

    return data.filter((col("session_id").isin(users)) & (col("event_type") == lit("request_draft-created"))).count()
