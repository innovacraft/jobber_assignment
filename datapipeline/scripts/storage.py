from lib.spark_setup import create_spark_session

def optimize_and_vacuum(spark, delta_path):
    """
    Optimize and clean up the Delta Lake table to improve performance and manage storage efficiently.
    """
    # Read processed data from Delta Lake
    delta_df = spark.read.format("delta").load(delta_path)
    print("Data read from Delta Lake at:", delta_path)

    # Optimize to compact files and improve query performance
    spark.sql(f"OPTIMIZE `{delta_path}` ZORDER BY (customer_id)")
    print("Optimization with ZORDER BY customer_id completed.")

    # Clean up old files beyond a retention period of 7 days
    spark.sql(f"VACUUM `{delta_path}` RETAIN 168 HOURS")  # Note: Spark SQL uses HOURS for VACUUM retention period
    print("VACUUM operation completed with a retention period of 7 days.")

    # Additional OPTIMIZE with ZORDER to improve performance on frequently queried columns
    spark.sql(f"""
        OPTIMIZE `{delta_path}`
        ZORDER BY (price_range)
    """)
    print("Additional optimization with ZORDER BY price_range completed.")

def main():
    """
    Main function to initialize Spark session and perform table maintenance operations.
    """
    spark = create_spark_session("Delta Lake Storage Management")
    delta_path = "s3://jobbertech/assignment/processed_data/customer_sales"
    optimize_and_vacuum(spark, delta_path)
    spark.stop()

if __name__ == "__main__":
    main()
