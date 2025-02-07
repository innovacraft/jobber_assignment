from pyspark.sql import SparkSession
from lib.spark_setup import create_spark_session
from lib.config_reader import get_redshift_connection_properties

def load_data_from_delta_and_write_to_redshift(spark):
    """
    Load data from Delta Lake and write it to Redshift for consumption.
    """
    # Retrieve Redshift connection settings from the config module
    redshift_url, properties = get_redshift_connection_properties()

    # Load data from Delta Lake
    delta_df = spark.read.format("delta").load("s3://jobbertech/assignment/processed_data/customer_sales")
    print("Data loaded from Delta Lake.")

    # Write to Redshift for consumption
    delta_df.write.jdbc(url=redshift_url, table="customer_sales", mode="append", properties=properties)
    print("Data written to Redshift table.")

def main():
    """
    Main function to initialize Spark session and execute data loading and writing operations.
    """
    spark = create_spark_session("Delta Lake to Redshift Consumption")
    load_data_from_delta_and_write_to_redshift(spark)
    spark.stop()

if __name__ == "__main__":
    main()
