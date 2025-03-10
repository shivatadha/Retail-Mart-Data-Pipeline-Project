import sys
import os
import shutil
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_fixed

from src.main.delete.local_file_delete import delete_local_file
from src.main.move.move_files import move_s3_to_s3
from src.main.transformation.job.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformation.job.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt
from resource.dev import config
from src.main.Read.aws_read import s3Reader
from src.main.Read.database_read import *
from src.main.utility.dataframe import *
from src.main.download.aws_file_download import S3FileDownloader
from src.main.utility.spark_session import spark_session
from src.main.Write.database_write import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.transformation.job.dimension_tables_join import dimesions_table_join
from src.main.Write.file_writer import FileWriter
from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Environment setup
os.environ['PYSPARK_PYTHON'] = 'D:\\python\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:\\python\\python.exe'

# Global variables
logger = logging.getLogger(__name__)


def initialize_s3_client():
    """Initialize and return an S3 client."""
    try:
        aws_access_key = decrypt(config.aws_access_key)
        aws_secret_key = decrypt(config.aws_secret_key)
        s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key)
        s3_client = s3_client_provider.get_client()
        buckets = s3_client.list_buckets()['Buckets']
        logger.info("S3 client initialized. Available buckets: %s", buckets)
        return s3_client
    except Exception as e:
        logger.error("Failed to initialize S3 client: %s", str(e), exc_info=True)
        raise


def check_local_files_and_staging():
    """Check local directory and staging table for existing files."""
    try:
        csv_files = [os.path.join(config.local_directory, f) for f in os.listdir(config.local_directory) if
                     f.endswith(".csv")]
        if not csv_files:
            logger.info("No local CSV files found. Last run assumed successful.")
            return []

        with get_mysql_connection() as conn, conn.cursor() as cursor:
            file_names = [os.path.basename(f) for f in csv_files]
            statement = f"""
            SELECT DISTINCT file_name FROM shop_project.product_staging_table
            WHERE file_name IN ({str(file_names)[1:-1]}) AND status = 'A'
            """
            logger.debug("Executing staging table query: %s", statement)
            cursor.execute(statement)
            active_files = [row[0] for row in cursor.fetchall()]
            if active_files:
                logger.warning("Active files from previous run found: %s", active_files)
                raise ValueError("Previous run failed. Check staging table for active files.")
            logger.info("No active files found in staging table.")
        return csv_files
    except Exception as e:
        logger.error("Error checking local files and staging: %s", str(e), exc_info=True)
        raise


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def download_s3_files(s3_client):
    """Download files from S3 with retry logic."""
    try:
        s3_reader = s3Reader()
        folder_path = config.s3_source_directory
        s3_files = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
        if not s3_files:
            logger.error("No files found in S3 at %s", folder_path)
            raise FileNotFoundError("No data available to process in S3.")

        prefix = f"s3://{config.bucket_name}/"
        file_paths = [url[len(prefix):] for url in s3_files]
        logger.info("Files to download from S3: %s", file_paths)

        downloader = S3FileDownloader(s3_client, config.bucket_name, config.local_directory)
        downloader.download_files(file_paths)
        downloaded_files = [os.path.join(config.local_directory, f) for f in os.listdir(config.local_directory) if
                            f.endswith(".csv")]
        logger.info("Downloaded %d files from S3: %s", len(downloaded_files), downloaded_files)
        return downloaded_files
    except Exception as e:
        logger.error("S3 download failed: %s", str(e), exc_info=True)
        raise


def validate_file_schemas(spark: SparkSession, files):
    """Validate schemas and segregate correct and error files."""
    try:
        if not files:
            logger.error("No files provided for schema validation.")
            raise ValueError("No files to validate.")

        correct_files, error_files = [], []
        mandatory_columns = set(config.mandatory_columns)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)

        for file in files:
            file_df = df.filter(df.input_file_name().endswith(os.path.basename(file)))
            schema = file_df.columns
            missing = mandatory_columns - set(schema)
            if missing:
                logger.warning("File %s missing columns: %s", file, missing)
                error_files.append(file)
            else:
                logger.debug("File %s has all mandatory columns", file)
                correct_files.append(file)

        logger.info("Correct files: %s | Error files: %s", correct_files, error_files)
        move_error_files(error_files)
        return correct_files
    except Exception as e:
        logger.error("Schema validation failed: %s", str(e), exc_info=True)
        raise


def move_error_files(error_files):
    """Move error files to local and S3 error directories."""
    try:
        if not error_files:
            logger.info("No error files to move.")
            return

        error_dir = config.error_folder_path_local
        os.makedirs(error_dir, exist_ok=True)
        for file in error_files:
            file_name = os.path.basename(file)
            dest = os.path.join(error_dir, file_name)
            if os.path.exists(file):
                shutil.move(file, dest)
                logger.info("Moved %s to local error dir: %s", file_name, dest)
                message = move_s3_to_s3(s3_client, config.bucket_name, config.s3_source_directory,
                                        config.s3_error_directory, file_name)
                logger.info("S3 move result: %s", message)
            else:
                logger.error("File not found: %s", file)
    except Exception as e:
        logger.error("Error moving files: %s", str(e), exc_info=True)
        raise


def update_staging_table_start(files):
    """Update staging table with 'Active' status for files."""
    try:
        if not files:
            logger.error("No files to process for staging table update.")
            raise ValueError("No valid files to update staging table.")

        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        statements = [
            f"INSERT INTO {config.database_name}.{config.product_staging_table} "
            f"(file_name, file_location, created_date, status) "
            f"VALUES ('{os.path.basename(f)}', '{os.path.basename(f)}', '{current_date}', 'A')"
            for f in files
        ]

        with get_mysql_connection() as conn, conn.cursor() as cursor:
            for stmt in statements:
                cursor.execute(stmt)
            conn.commit()
        logger.info("Staging table updated with 'Active' status for %d files.", len(files))
    except Exception as e:
        logger.error("Failed to update staging table: %s", str(e), exc_info=True)
        raise


def process_data(spark: SparkSession, files):
    """Process and enrich data from CSV files."""
    try:
        if not files:
            logger.error("No correct files to process.")
            raise ValueError("No data to process.")

        # Batch read and handle extra columns
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)
        extra_cols = [c for c in df.columns if c not in config.mandatory_columns]
        if extra_cols:
            df = df.withColumn("additional_column", concat_ws(", ", *extra_cols))
            logger.info("Added 'additional_column' for extra columns: %s", extra_cols)
        else:
            df = df.withColumn("additional_column", lit(None))
            logger.info("No extra columns found, 'additional_column' set to null.")

        final_df = df.select(
            "customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
            "price", "quantity", "total_cost", "additional_column"
        )
        final_df.cache()  # Cache for performance

        # Load dimension tables
        customer_df = customer_table_df()
        product_df = product_table_df()
        staging_df = product_staging_table_df()
        sales_team_df = sales_team_table_df()
        store_df = store_table_df()

        # Enrich data
        enriched_df = dimesions_table_join(final_df, customer_df, store_df, sales_team_df)
        logger.info("Data enriched with dimension tables successfully.")
        return enriched_df
    except Exception as e:
        logger.error("Data processing failed: %s", str(e), exc_info=True)
        raise


def create_customer_data_mart(enriched_df, s3_client):
    """Create and store customer data mart."""
    try:
        customer_df = enriched_df.select(
            "ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
            "ct.pincode", "phone_number", "sales_date", "total_cost"
        )
        writer = FileWriter("overwrite", "parquet")
        writer.dataframe_writer(customer_df, config.customer_data_mart_local_file)
        logger.info("Customer data mart written to %s", config.customer_data_mart_local_file)

        s3_uploader = UploadToS3(s3_client)
        message = s3_uploader.upload_to_s3(config.s3_customer_datamart_directory, config.bucket_name,
                                           config.customer_data_mart_local_file)
        logger.info("Customer data mart uploaded to S3: %s", message)

        customer_mart_calculation_table_write(customer_df)
        logger.info("Customer mart calculations written to MySQL.")
    except Exception as e:
        logger.error("Customer data mart creation failed: %s", str(e), exc_info=True)
        raise


def create_sales_data_mart(enriched_df, s3_client):
    """Create and store sales team data mart."""
    try:
        sales_df = enriched_df.select(
            "store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
            "store_manager_name", "manager_id", "is_manager", "sales_person_address", "sales_person_pincode",
            "sales_date", "total_cost", expr("SUBSTRING(sales_date, 1, 7) as sales_month")
        )
        writer = FileWriter("overwrite", "parquet")
        writer.dataframe_writer(sales_df, config.sales_team_data_mart_local_file)
        logger.info("Sales data mart written to %s", config.sales_team_data_mart_local_file)

        s3_uploader = UploadToS3(s3_client)
        message = s3_uploader.upload_to_s3(config.s3_sales_datamart_directory, config.bucket_name,
                                           config.sales_team_data_mart_local_file)
        logger.info("Sales data mart uploaded to S3: %s", message)

        # Partitioned write
        sales_df.write.format("parquet").mode("overwrite").partitionBy("sales_month", "store_id").save(
            config.sales_team_data_mart_partitioned_local_file)
        logger.info("Partitioned sales data mart written to %s", config.sales_team_data_mart_partitioned_local_file)
        upload_partitioned_files(s3_client, config.sales_team_data_mart_partitioned_local_file,
                                 "sales_partitioned_data_mart")

        sales_mart_calculation_table_write(sales_df)
        logger.info("Sales mart calculations written to MySQL.")
    except Exception as e:
        logger.error("Sales data mart creation failed: %s", str(e), exc_info=True)
        raise


def upload_partitioned_files(s3_client, local_dir, s3_prefix):
    """Upload partitioned files to S3 in parallel."""
    try:
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        files_to_upload = [
            (os.path.join(root, f), f"{s3_prefix}/{current_epoch}/{os.path.relpath(os.path.join(root, f), local_dir)}")
            for root, _, files in os.walk(local_dir) for f in files
        ]
        if not files_to_upload:
            logger.info("No partitioned files to upload.")
            return

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(lambda x: s3_client.upload_file(x[0], config.bucket_name, x[1]), files_to_upload)
        logger.info("Uploaded %d partitioned files to S3 under %s", len(files_to_upload), s3_prefix)
    except Exception as e:
        logger.error("Partitioned file upload failed: %s", str(e), exc_info=True)
        raise


def finalize_processing(s3_client, files):
    """Move processed files, clean up local files, and update staging table."""
    try:
        # Move S3 files to processed directory
        message = move_s3_to_s3(s3_client, config.bucket_name, config.s3_source_directory,
                                config.s3_processed_directory)
        logger.info("S3 files moved to processed directory: %s", message)

        # Clean up local files
        paths_to_clean = [
            config.local_directory,
            config.customer_data_mart_local_file,
            config.sales_team_data_mart_local_file,
            config.sales_team_data_mart_partitioned_local_file
        ]
        for path in paths_to_clean:
            try:
                delete_local_file(path)
                logger.info("Cleaned up local path: %s", path)
            except Exception as e:
                logger.warning("Failed to clean up %s: %s", path, str(e))

        # Update staging table to 'Inactive'
        if files:
            current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            statements = [
                f"UPDATE {config.database_name}.{config.product_staging_table} "
                f"SET status = 'I', updated_date = '{current_date}' "
                f"WHERE file_name = '{os.path.basename(f)}'"
                for f in files
            ]
            with get_mysql_connection() as conn, conn.cursor() as cursor:
                for stmt in statements:
                    cursor.execute(stmt)
                conn.commit()
            logger.info("Staging table updated to 'Inactive' for %d files.", len(files))
        else:
            logger.warning("No files processed to update staging table.")
    except Exception as e:
        logger.error("Finalization failed: %s", str(e), exc_info=True)
        raise


def main():
    """Main function to orchestrate the ETL pipeline."""
    try:
        # Initialize resources
        global s3_client
        s3_client = initialize_s3_client()
        spark = spark_session()
        logger.info("Spark session created successfully.")

        # Check and download files
        local_files = check_local_files_and_staging()
        s3_files = download_s3_files(s3_client)
        all_files = list(set(local_files + s3_files))  # Combine and deduplicate

        # Process files
        correct_files = validate_file_schemas(spark, all_files)
        update_staging_table_start(correct_files)
        enriched_df = process_data(spark, correct_files)

        # Create data marts
        create_customer_data_mart(enriched_df, s3_client)
        create_sales_data_mart(enriched_df, s3_client)

        # Finalize
        finalize_processing(s3_client, correct_files)
        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.error("Pipeline failed: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()