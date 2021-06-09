from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower


def apply_filters(data: DataFrame):
    return data \
        .filter(lower(col('env')) == 'production') \
        .filter(data['is_internal_ip'] == False) \
        .filter((col('session_id').isNotNull()) & (col('page_type').isNotNull()) & (col('event_type').isNotNull()))
