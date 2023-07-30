import dlt

# load files from cloud object storage
@dlt.table
def customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/databricks-datasets/retail-org/customers/")
    )

@dlt.table
def sales_orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/databricks-datasets/retail-orgs/sales_orders")
    )

# load data from a message bus
@dlt.table
def kafka_raw():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "<server:ip>")
        .option("subscribe","topic1")
        .option("startingOffsets", "latest")
        .load()
    )

# load data from external systems
@dlt.table
def postgres_raw():
    return (
        spark.read
        .format("postgresql")
        .option("dbtable", table_name)
        .option("host", database_host_url)
        .option("port",5432)
        .option("database",database_name)
        .option("user",username)
        .option("password", password)
        .load()
    )

# load small or static datasets from cloud object storage
@dlt.table
def clickstream_raw():
    return (
        spark.read
        .format("json")
        .load(json_path)
    )