from pyspark.sql import SparkSession

def create_spark_session(app_name):
    """
    Initialize a Spark session for processing with AWS credentials and Delta Lake configurations
    for use within the Databricks environment.
    """
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Set AWS S3 access credentials using Databricks secrets directly
    spark.conf.set("spark.hadoop.fs.s3a.access.key", dbutils.secrets.get(scope="aws-keys", key="aws-access-key"))
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", dbutils.secrets.get(scope="aws-keys", key="aws-secret-key"))

    return spark
