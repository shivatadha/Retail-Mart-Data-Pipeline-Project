import sys
import os
from src.main.delete.local_file_delete import delete_local_file

os.environ['PYSPARK_PYTHON'] = 'D:\\python\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:\\python\\python.exe'
from src.main.move.move_files import move_s3_to_s3
from src.main.transformation.job.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformation.job.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import decrypt, encrypt
from resource.dev import config
from src.main.Read.aws_read import *
from src.main.Read.database_read import *
from src.main.utility.dataframe import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import *
from src.main.Write.database_write import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.transformation.job.dimension_tables_join import dimesions_table_join
from src.main.Write.file_writer import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.window import Window
import shutil
import datetime
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_fixed

# Initialize S3 client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()


# Function to validate local files against staging table
def validate_local_files():
    """Check local directory for existing files and validate against staging table."""
    csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
    connection = get_mysql_connection()
    cursor = connection.cursor()

    if csv_files:
        statement = f"""
        select distinct file_name from
        shop_project.product_staging_table
        where file_name in ({str(csv_files)[1:-1]}) and status = 'A'
        """
        logger.debug(f"Dynamically created statement: {statement}")
        cursor.execute(statement)
        data = cursor.fetchall()
        if data:
            logger.warning("Your last run failed, please check staging table for active files")
        else:
            logger.info("No active records found in staging table")
    else:
        logger.info("Last run was successful, no local CSV files found")

    cursor.close()
    connection.close()
    return csv_files


# Function to list and download S3 files with retry logic
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def process_s3_files():
    """List and download files from S3 source directory."""
    try:
        s3_reader = s3Reader()
        folder_path = config.s3_source_directory
        s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
        logger.info("Absolute S3 paths: %s", s3_absolute_file_path)

        if not s3_absolute_file_path:
            logger.error(f"No files available at {folder_path}")
            raise Exception("No data available to process")

        bucket_name = config.bucket_name
        local_directory = config.local_directory
        prefix = f"s3://{bucket_name}/"
        file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
        logger.info(f"Files to download from S3: {file_paths}")

        downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
        downloader.download_files(file_paths)

        all_files = os.listdir(local_directory)
        logger.info(f"Files downloaded to local directory: {all_files}")
        return all_files

    except Exception as e:
        logger.error("S3 processing failed: %s", e)
        raise


# Function to validate and segregate files
def segregate_files(all_files):
    """Segregate downloaded files into CSV and error files."""
    if not all_files:
        logger.error("No files downloaded to process")
        raise Exception("No data to process")

    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory, files)))

    if not csv_files:
        logger.error("No CSV files available to process")
        raise Exception("No CSV data available to process")

    csv_files_str = str(csv_files)[1:-1]
    logger.info("CSV files to process: %s", csv_files_str)
    return csv_files, error_files


# Function to validate schema and move error files
def validate_schema_and_move_errors(csv_files, spark):
    """Validate CSV schemas and move error files."""
    error_files = []
    correct_files = []
    for data in csv_files:
        data_schema = spark.read.format("csv").option("header", "true").load(data).columns
        logger.debug(f"Schema for {data}: {data_schema}")
        missing_columns = set(config.mandatory_columns) - set(data_schema)
        logger.debug(f"Missing columns in {data}: {missing_columns}")

        if missing_columns:
            error_files.append(data)
        else:
            logger.info(f"No missing columns for {data}")
            correct_files.append(data)

    logger.info(f"Correct files: {correct_files}")
    logger.info(f"Error files: {error_files}")

    error_folder_local_path = config.error_folder_path_local
    if error_files:
        os.makedirs(error_folder_local_path, exist_ok=True)
        for file_path in error_files:
            if os.path.exists(file_path):
                file_name = os.path.basename(file_path)
                destination_path = os.path.join(error_folder_local_path, file_name)
                shutil.move(file_path, destination_path)
                logger.info(f"Moved error file '{file_name}' to '{destination_path}'")

                source_prefix = config.s3_source_directory
                destination_prefix = config.s3_error_directory
                message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
                logger.info(message)
            else:
                logger.error(f"File not found: {file_path}")
    else:
        logger.info("No error files detected")

    return correct_files


# Function to update staging table
def update_staging_table(correct_files, status='A'):
    """Update product_staging_table with file status."""
    insert_statements = []
    db_name = config.database_name
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

    if not correct_files:
        logger.error("No correct files to process")
        raise Exception("No data available with correct files")

    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table} " \
                     f"(file_name, file_location, created_date, status)" \
                     f" VALUES ('{filename}', '{filename}', '{formatted_date}', '{status}')"
        insert_statements.append(statements)

    logger.debug(f"Staging table insert statements: {insert_statements}")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
    logger.info("Staging table updated successfully")


# Function to process CSV data into a single DataFrame
def process_csv_data(correct_files, spark):
    """Process CSV files into a unified DataFrame with additional columns handled."""
    final_df_to_process = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(correct_files)
    final_df_to_process.cache()  # Cache for performance

    data_schema = final_df_to_process.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.debug(f"Extra columns detected: {extra_columns}")

    if extra_columns:
        final_df_to_process = final_df_to_process.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
        logger.info("Added 'additional_column' for extra columns")
    else:
        final_df_to_process = final_df_to_process.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
        logger.info("No extra columns, added null 'additional_column'")

    logger.info("Final DataFrame ready for processing")
    return final_df_to_process


# Function to enrich data
def enrich_data(final_df_to_process):
    """Enrich DataFrame with dimension tables."""
    customer_table_df = customer_table_df()
    product_table_df = product_table_df()
    product_staging_table_df = product_staging_table_df()
    sales_team_table_df = sales_team_table_df()
    store_table_df = store_table_df()

    logger.info("Loaded dimension tables")
    s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                           customer_table_df,
                                                           store_table_df,
                                                           sales_team_table_df)
    logger.info("Data enriched with dimension tables")
    return s3_customer_store_sales_df_join


# Function to process and write data marts
def process_data_marts(s3_customer_store_sales_df_join):
    """Process and write customer and sales team data marts."""
    # Customer Data Mart
    final_customer_data_mart_df = s3_customer_store_sales_df_join \
        .select("ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
                "ct.pincode", "phone_number", "sales_date", "total_cost")
    logger.info("Customer data mart prepared")

    parquet_writer = FileWriter("overwrite", "parquet")
    parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)
    logger.info(f"Customer data written to {config.customer_data_mart_local_file}")

    s3_uploader = UploadToS3(s3_client)
    s3_directory = config.s3_customer_datamart_directory
    message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
    logger.info(message)

    # Sales Team Data Mart
    final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
        .select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
                "store_manager_name", "manager_id", "is_manager", "sales_person_address",
                "sales_person_pincode", "sales_date", "total_cost",
                expr("SUBSTRING(sales_date,1,7) as sales_month"))
    logger.info("Sales team data mart prepared")

    parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
    logger.info(f"Sales team data written to {config.sales_team_data_mart_local_file}")

    s3_directory = config.s3_sales_datamart_directory
    message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
    logger.info(message)

    # Partitioned Sales Team Data
    final_sales_team_data_mart_df.write.format("parquet") \
        .option("header", "true") \
        .mode("overwrite") \
        .partitionBy("sales_month", "store_id") \
        .option("path", config.sales_team_data_mart_partitioned_local_file) \
        .save()
    logger.info("Partitioned sales team data written")

    s3_prefix = "sales_partitioned_data_mart"
    current_epoch = int(datetime.datetime.now().timestamp()) * 1000
    with ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(local_file_path,
                                                     config.sales_team_data_mart_partitioned_local_file)
                s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
                executor.submit(s3_client.upload_file, local_file_path, config.bucket_name, s3_key)
    logger.info("Partitioned sales team data uploaded to S3")

    return final_customer_data_mart_df, final_sales_team_data_mart_df


# Function to calculate and write metrics
def calculate_metrics(final_customer_data_mart_df, final_sales_team_data_mart_df):
    """Calculate and write metrics to MySQL."""
    logger.info("Calculating customer monthly purchase amounts")
    customer_mart_calculation_table_write(final_customer_data_mart_df)
    logger.info("Customer mart calculations completed")

    logger.info("Calculating sales team monthly billed amounts")
    sales_mart_calculation_table_write(final_sales_team_data_mart_df)
    logger.info("Sales mart calculations completed")


# Function to finalize process
def finalize_process(correct_files):
    """Move files to processed folder, clean up local files, and update staging table."""
    source_prefix = config.s3_source_directory
    destination_prefix = config.s3_processed_directory
    message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
    logger.info(message)

    for path in [config.local_directory, config.customer_data_mart_local_file,
                 config.sales_team_data_mart_local_file, config.sales_team_data_mart_partitioned_local_file]:
        try:
            delete_local_file(path)
            logger.info(f"Deleted local files at {path}")
        except Exception as e:
            logger.error(f"Failed to delete {path}: {e}")

    update_statements = []
    db_name = config.database_name
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

    if correct_files:
        for file in correct_files:
            filename = os.path.basename(file)
            statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                         f"SET status = 'I', updated_date='{formatted_date}' " \
                         f"WHERE file_name = '{filename}'"
            update_statements.append(statements)

        logger.debug(f"Staging table update statements: {update_statements}")
        connection = get_mysql_connection()
        cursor = connection.cursor()
        for statement in update_statements:
            cursor.execute(statement)
            connection.commit()
        cursor.close()
        connection.close()
        logger.info("Staging table updated to inactive status")
    else:
        logger.error("No files processed, check earlier steps")
        sys.exit(1)


# Main execution workflow
def main():
    """Main ETL pipeline execution."""
    try:
        # Validate local files
        csv_files = validate_local_files()

        # Process S3 files
        all_files = process_s3_files()

        # Segregate files
        csv_files, error_files = segregate_files(all_files)

        # Create Spark session
        logger.info("Creating Spark session")
        spark = spark_session()
        spark.conf.set("spark.sql.shuffle.partitions", 50)  # Optimize partitioning
        logger.info("Spark session created")

        # Validate schema and move errors
        correct_files = validate_schema_and_move_errors(csv_files, spark)

        # Update staging table with active status
        update_staging_table(correct_files)

        # Process CSV data
        final_df_to_process = process_csv_data(correct_files, spark)

        # Enrich data
        s3_customer_store_sales_df_join = enrich_data(final_df_to_process)

        # Process data marts
        final_customer_data_mart_df, final_sales_team_data_mart_df = process_data_marts(s3_customer_store_sales_df_join)

        # Calculate metrics
        calculate_metrics(final_customer_data_mart_df, final_sales_team_data_mart_df)

        # Finalize process
        finalize_process(correct_files)

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()