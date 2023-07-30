import dlt
from pyspark.sql.functions import *

json_path = ""

# create table from files
dlt.table(
    comment="the raw wikipedia clickstream dataset, ingested from databrick dataset."
)
def clickstream_raw():
    return (spark.read.format("json").load(json_path))

# add table from upstream dataset
@dlt.table(
    comment="wikipedia clickstream data cleaned and prepared for analysis."
)
@dlt.expect("valid_current_page_tittle", "current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")
def clickstream_prepared():
    return (
        dlt.read("clickstream_raw")
        .withColumn("click_count", expr("CAST(n AS INT)"))
        .withColumnRenamed("curr_title", "current_page_title")
        .withColumnRenamed("prev_title", "previous_page_title")
        .select("current_page_title", "click_count", "previous_page_title")
    )

# create table with enriched data
@dlt.table(
    comment = "table containing top pages linking to Apache spark page"
)
def top_spark_referrer():
    return (
        dlt.read("clickstream_prepared")
        .filter(expr("current_page_title=='Apache Spark'"))
        .withColumnRenamed("previous_page_title", "referrer")
        .sort(desc("click_count"))
        .select("referrer", "click_count")
        .limit(10)
    )