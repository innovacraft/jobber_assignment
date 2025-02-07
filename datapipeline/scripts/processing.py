from lib.spark_setup import create_spark_session
from pyspark.sql.functions import col, current_date, datediff, to_date, when
from delta.tables import DeltaTable

def read_staged_data(spark):
    """
    Read staged data from S3 for processing.
    """
    customer_df = spark.read.parquet("s3://jobbertech/assignment/stage/customer/")
    sales_df = spark.read.parquet("s3://jobbertech/assignment/stage/sales/")
    return customer_df, sales_df

def process_data(customer_df, sales_df):
    """
    Perform transformations and enrichments on the data.
    """
    processed_df = sales_df.join(customer_df, "customer_id")
    processed_df = processed_df.withColumn("age_group", (col("age") / 10).cast("integer") * 10)
    processed_df = processed_df.withColumn("days_since_purchase", datediff(current_date(), to_date(col("invoice_date"), "yyyy-MM-dd")))
    processed_df = processed_df.withColumn("total_sales", col("quantity") * col("price"))
    processed_df = processed_df.withColumn("price_range",
                                   when(col("price") <= 500, "Budget")
                                   .when((col("price") > 500) & (col("price") <= 1500), "Mid-range")
                                   .otherwise("Premium"))
    return processed_df

def write_and_merge_data(spark, processed_df):
    """
    Write processed data to Delta Lake and perform upserts using merge operations.
    """
    processed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save("s3://jobbertech/assignment/processed_data/customer_sales")

    updates_df = spark.read.format("delta").load("s3://jobbertech/assignment/updates/")
    delta_table = DeltaTable.forPath(spark, "s3://jobbertech/assignment/processed_data/customer_sales")

    delta_table.alias("original").merge(
        updates_df.alias("updates"),
        "original.customer_id = updates.customer_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def run_processing():
    """
    Orchestrate the data processing workflow using modular functions above.
    """
    spark = create_spark_session("Data Processing Job")
    customer_df, sales_df = read_staged_data(spark)
    processed_df = process_data(customer_df, sales_df)
    write_and_merge_data(spark, processed_df)
    spark.stop()

if __name__ == "__main__":
    run_processing()
