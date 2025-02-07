from lib.spark_setup import create_spark_session
from pyspark.sql.functions import col, current_date, datediff, to_date, when
from delta.tables import DeltaTable

def process_data(spark):
    """Read, process, and write data using Delta Lake."""
    # Read staged data
    customer_df = spark.read.parquet("s3://jobbertech/assignment/stage/customer/")
    sales_df = spark.read.parquet("s3://jobbertech/assignment/stage/sales/")

    # Data transformations and enrichments
    processed_df = sales_df.join(customer_df, "customer_id")
    processed_df = processed_df.withColumn("age_group", (col("age") / 10).cast("integer") * 10)
    processed_df = processed_df.withColumn("days_since_purchase", datediff(current_date(), to_date(col("invoice_date"), "yyyy-MM-dd")))
    processed_df = processed_df.withColumn("total_sales", col("quantity") * col("price"))
    processed_df = processed_df.withColumn("price_range",
                                   when(col("price") <= 500, "Budget")
                                   .when((col("price") > 500) & (col("price") <= 1500), "Mid-range")
                                   .otherwise("Premium"))

    # Write processed data to Delta Lake
    processed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save("s3://jobbertech/assignment/processed_data/customer_sales")

    # Assuming updates are stored in another bucket, prepare for merge operations
    updates_df = spark.read.format("delta").load("s3://jobbertech/assignment/updates/")
    delta_table = DeltaTable.forPath(spark, "s3://jobbertech/assignment/processed_data/customer_sales")

    # Perform upserts using MERGE
    (delta_table.alias("original")
     .merge(
         updates_df.alias("updates"),
         "original.customer_id = updates.customer_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

def main():
    spark = create_spark_session("Data Processing Job")
    process_data(spark)
    spark.stop()

if __name__ == "__main__":
    main()
