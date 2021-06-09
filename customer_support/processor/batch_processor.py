from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, regexp_replace, concat_ws, collect_list

from customer_support.client.mongo_client import save
from customer_support.processor.covid19_info_extractor import total_cases


# get all tweets and remove #, RT: and urls add one column for timestamp and another for total coronavirus cases
def process_batch(batch: DataFrame, epoch_id: int):
    message: DataFrame = batch \
        .agg(concat_ws(", ", collect_list("value")).alias("tweets")) \
        .withColumn("content", regexp_replace("tweets", "http\S+|www.\S+|#|RT:", "")) \
        .select("content") \
        .withColumn("timestamp", current_timestamp()) \
        .withColumn("total_case_count", total_cases())
    save(message)
