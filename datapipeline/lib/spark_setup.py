import configparser
from pyspark.sql import SparkSession

def create_spark_session():
    config = configparser.ConfigParser()
    config.read('config/spark.conf')

    spark = SparkSession.builder.appName(config['spark']['app_name']) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", config['spark']['aws_access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['spark']['aws_secret_access_key']) \
        .config("spark.jars.packages", f"io.delta:delta-core_2.12:{config['spark']['delta_version']}") \
        .getOrCreate()

    return spark
