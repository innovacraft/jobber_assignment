from lib.spark_setup import create_spark_session

def read_delta_data(spark, delta_path):
    """
    Read data from a Delta Lake table.
    """
    delta_df = spark.read.format("delta").load(delta_path)
    print("Data read from Delta Lake at:", delta_path)
    return delta_df

def optimize_delta_table(spark, delta_path):
    """
    Optimize the Delta Lake table to compact files and improve query performance.
    """
    spark.sql(f"OPTIMIZE `{delta_path}` ZORDER BY (customer_id)")
    print("Optimization with ZORDER BY customer_id completed.")
    spark.sql(f"""
        OPTIMIZE `{delta_path}`
        ZORDER BY (price_range)
    """)
    print("Additional optimization with ZORDER BY price_range completed.")

def vacuum_delta_table(spark, delta_path):
    """
    Clean up old files beyond a retention period to manage storage efficiently.
    """
    spark.sql(f"VACUUM `{delta_path}` RETAIN 168 HOURS")
    print("VACUUM operation completed with a retention period of 7 days.")

def run_storage():
    """
    Orchestrate the optimization and cleaning of Delta Lake tables.
    """
    spark = create_spark_session("Delta Lake Storage Management")
    delta_path = "s3://jobbertech/assignment/processed_data/customer_sales"
    read_delta_data(spark, delta_path)
    optimize_delta_table(spark, delta_path)
    vacuum_delta_table(spark, delta_path)
    spark.stop()

if __name__ == "__main__":
    run_storage()
