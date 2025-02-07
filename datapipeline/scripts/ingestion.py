from lib.spark_setup import create_spark_session

def read_data(spark):
    """
    Read data from S3 using the Spark session with pre-configured AWS credentials.
    """
    # Reading sales data from S3 in CSV format
    sales_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("s3a://jobbertech/assignment/data/sales/")

    # Reading customer data from S3 in Parquet format
    customer_df = spark.read.format("parquet") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("s3://jobbertech/assignment/data/customer/")

    return sales_df, customer_df

def write_data_to_stage(sales_df, customer_df):
    """
    Write data back to S3 in a staging area in Parquet format.
    """
    # Writing customer data to S3 in Parquet format for staging
    customer_df.write \
        .mode("overwrite") \
        .parquet("s3://jobbertech/assignment/stage/customer/")

    # Writing sales data to S3 in Parquet format for staging
    sales_df.write \
        .mode("overwrite") \
        .parquet("s3://jobbertech/assignment/stage/sales/")

def main():
    spark = create_spark_session("Ingestion Job")
    sales_df, customer_df = read_data(spark)
    write_data_to_stage(sales_df, customer_df)
    spark.stop()

if __name__ == "__main__":
    main()
