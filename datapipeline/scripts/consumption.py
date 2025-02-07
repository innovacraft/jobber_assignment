from pyspark.sql import SparkSession
from lib.spark_setup import create_spark_session
from lib.config_reader import get_redshift_connection_properties

def read_data_from_delta(spark):
    """
    Read data from Delta Lake.
    """
    delta_df = spark.read.format("delta").load("s3://jobbertech/assignment/processed_data/customer_sales")
    print("Data loaded from Delta Lake.")
    return delta_df

def write_data_to_redshift(spark, delta_df):
    """
    Write data to Redshift.
    """
    redshift_url, properties = get_redshift_connection_properties()
    delta_df.write.jdbc(url=redshift_url, table="customer_sales", mode="append", properties=properties)
    print("Data written to Redshift table.")

def run_consumption():
    """
    Orchestrate the loading of data from Delta Lake and writing to Redshift.
    """
    spark = create_spark_session("Delta Lake to Redshift Consumption")
    delta_df = read_data_from_delta(spark)
    write_data_to_redshift(spark, delta_df)
    spark.stop()

if __name__ == "__main__":
    run_consumption()
