# Data Pipeline Project

## Project Description
This project develops a robust data pipeline to process and analyze customer and sales data. The resulting dataset is stored in Delta Lake, optimized for high performance and ready for analytical use.

## How to Use This Repository

### Structure
- `data/`: Contains raw and processed data in various stages of the pipeline.
- `notebooks/`: Databricks notebooks for step-by-step processing of data.
- `scripts/`: Python scripts for automated data processing.
- `config/`: Configuration files for database connections.
- `lib/`: Helper libraries for Spark session setup and configuration reading.
- `docs/`: Additional project documentation.
- `airflow/`: Airflow DAGs, configuration, and logs for orchestrating the pipeline.


### Setup
1. Ensure AWS credentials are configured to allow access to S3 and Redshift.
2. Install necessary Python packages: `pyspark`, `delta`, and `configparser`.
3. Configure Spark settings to use Delta Lake for the storage layer.
4. Set up Airflow on a local or cloud environment and configure it using the `airflow.cfg` and `webserver_config.py` files.

### Running the Pipeline
Execute scripts in the `scripts/` directory in sequence:
1. `ingestion.py` - Load data from raw sources.
2. `processing.py` - Transform data according to business requirements.
3. `storage.py` - Store and optimize data in Delta Lake.
4. `consumption.py` - Load optimized data into Redshift for analysis.
5. Schedule and monitor workflows using Airflow. Access the Airflow webserver to view task logs and pipeline status.

### Monitoring and Maintenance
Monitor the pipeline using AWS CloudWatch and maintain it by regularly checking logs and performance metrics. Use Airflow's built-in monitoring tools to track task execution and troubleshoot failures.

## Contributing
This project is developed as an assignment for Jobber and is intended to demonstrate data pipeline capabilities. While contributions to improve the pipeline or extend its functionality are welcome, please note that this repository is primarily for educational and demonstrative purposes. Please submit pull requests or open issues to propose changes.

## License
The use of this project for commercial purposes without prior agreement with Jobber is not authorized.
