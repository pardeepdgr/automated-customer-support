from pyspark.sql import DataFrame
from pyspark.sql.functions import col

r"""CREATE EXTERNAL TABLE test_table_json (
    hash STRING,
    detection STRUCT <
        filename: STRING,
        filepath: STRING,
        filesize: BIGINT,
        name: STRING
    >,
    metadata STRUCT <
        cmdline: STRING,
        country: STRING,
        os_name: STRING,
        parentsize: BIGINT,
        timestamp: TIMESTAMP
    >
)
PARTITIONED BY(dt STRING)
CLUSTERED BY(hash) SORTED BY(hash) INTO 2 BUCKETS
ROW FORMAT
SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://test/test-table/';"""


def extract(df: DataFrame):
    return df.select(
        col("metadata").getItem("country").alias("country"),
        col("detection").getItem("name").alias("detection")
    )
