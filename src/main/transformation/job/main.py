import sys
import os

from resource.dev import config
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
from src.main.Read.aws_read import *
from src.main.Read.database_read import *
from src.main.utility.dataframe import (customer_table, product_table, product_staging_table,
                                        sales_team_table, store_table, final_df)
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


def setup_s3_client():
    """Setup S3 client configuration"""
    try:
        aws_access_key = config.aws_access_key
        aws_secret_key = config.aws_secret_key
        s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
        s3_client = s3_client_provider.get_client()
        response = s3_client.list_buckets()
        logger.info("S3 client initialized successfully. Buckets available: %s", [bucket['Name'] for bucket in response['Buckets']])
        return s3_client
    except Exception as e:
        logger.error("Failed to initialize S3 client: %s", str(e))
        raise


def check_local_files():
    """Check local directory files against staging table"""
    csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()

        if csv_files:
            statement = f"""
            select distinct file_name from
            shop_project.product_staging_table
            where file_name in ({str(csv_files)[1:-1]}) and status = 'A'
            """
            logger.debug("Executing SQL query: %s", statement)
            cursor.execute(statement)
            data = cursor.fetchall()
            if data:
                logger.warning("Previous run failed. Files with status 'A' found: %s", data)
            else:
                logger.info("No matching records found in staging table.")
        else:
            logger.info("No CSV files found in local directory. Last run was successful.")

        cursor.close()
        connection.close()
    except Exception as e:
        logger.error("Error while checking local files: %s", str(e))
        raise
    finally:
        if 'connection' in locals():
            connection.close()


def list_s3_files(s3_client):
    """List files in S3 bucket"""
    try:
        s3_reader = s3Reader()
        folder_path = config.s3_source_directory
        s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
        logger.info("Files listed from S3 bucket '%s' under folder '%s': %s", config.bucket_name, folder_path, s3_absolute_file_path)
        if not s3_absolute_file_path:
            logger.warning("No files found in S3 folder: %s", folder_path)
            raise Exception("No data available to process.")
        return s3_absolute_file_path
    except Exception as e:
        logger.error("Failed to list files from S3: %s", str(e))
        raise


def download_s3_files(s3_client, s3_absolute_file_path):
    """Download files from S3"""
    bucket_name = config.bucket_name
    local_directory = config.local_directory
    prefix = f"s3://{bucket_name}/"
    file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
    logger.info("Downloading files from S3 bucket '%s': %s", bucket_name, file_paths)

    try:
        downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
        downloader.download_files(file_paths)
        logger.info("Files downloaded successfully to local directory: %s", local_directory)
    except Exception as e:
        logger.error("Failed to download files from S3: %s", str(e))
        raise


def process_local_files():
    """Process downloaded files"""
    all_files = os.listdir(config.local_directory)
    logger.info("Files found in local directory: %s", all_files)

    try:
        if all_files:
            csv_files = []
            error_files = []
            for files in all_files:
                if files.endswith(".csv"):
                    csv_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
                else:
                    error_files.append(os.path.abspath(os.path.join(config.local_directory, files)))

            if not csv_files:
                logger.error("No CSV files found to process.")
                raise Exception("No CSV data available to process.")
            return csv_files, error_files
        else:
            logger.error("No files found in local directory.")
            raise Exception("No data available to process.")
    except Exception as e:
        logger.error("Error while processing local files: %s", str(e))
        raise


def create_spark_session():
    """Create Spark session"""
    try:
        logger.info("Initializing Spark session...")
        spark = spark_session()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error("Failed to create Spark session: %s", str(e))
        raise


def check_file_schemas(spark, csv_files):
    """Check schemas of CSV files"""
    logger.info("Validating schemas for CSV files...")
    error_files = []
    correct_files = []

    try:
        for data in csv_files:
            data_schema = spark.read.format("csv") \
                .option("header", "true") \
                .load(data).columns
            logger.debug("Schema for file '%s': %s", data, data_schema)
            missing_columns = set(config.mandatory_columns) - set(data_schema)
            logger.debug("Missing columns in file '%s': %s", data, missing_columns)

            if missing_columns:
                error_files.append(data)
                logger.warning("File '%s' is missing mandatory columns: %s", data, missing_columns)
            else:
                correct_files.append(data)
                logger.info("File '%s' has all mandatory columns.", data)

        logger.info("Files with valid schemas: %s", correct_files)
        logger.info("Files with schema errors: %s", error_files)
        return correct_files, error_files
    except Exception as e:
        logger.error("Schema validation failed: %s", str(e))
        raise


def move_error_files(s3_client, error_files):
    """Move error files to error directory"""
    logger.info("Moving error files to error directory...")
    error_folder_local_path = config.error_folder_path_local

    try:
        if error_files:
            for file_path in error_files:
                if os.path.exists(file_path):
                    file_name = os.path.basename(file_path)
                    destination_path = os.path.join(error_folder_local_path, file_name)
                    shutil.move(file_path, destination_path)
                    logger.info("Moved error file '%s' to '%s'.", file_name, destination_path)

                    source_prefix = config.s3_source_directory
                    destination_prefix = config.s3_error_directory
                    message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
                    logger.info("S3 move operation result: %s", message)
                else:
                    logger.error("File '%s' does not exist.", file_path)
        else:
            logger.info("No error files to move.")
    except Exception as e:
        logger.error("Failed to move error files: %s", str(e))
        raise


def update_staging_table_start(correct_files):
    """Update staging table with start status"""
    logger.info("Updating staging table with start status...")
    insert_statements = []
    db_name = config.database_name
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

    try:
        if correct_files:
            for file in correct_files:
                filename = os.path.basename(file)
                statements = f"INSERT INTO {db_name}.{config.product_staging_table} " \
                             f"(file_name, file_location,created_date, status)" \
                             f" VALUES ('{filename}', '{filename}','{formatted_date}' ,'A')"
                insert_statements.append(statements)

            logger.debug("Generated SQL statements: %s", insert_statements)
            connection = get_mysql_connection()
            cursor = connection.cursor()

            for statement in insert_statements:
                cursor.execute(statement)
                connection.commit()
            cursor.close()
            connection.close()
            logger.info("Staging table updated successfully.")
        else:
            logger.error("No valid files to process.")
            raise Exception("No data available with correct files.")
    except Exception as e:
        logger.error("Failed to update staging table: %s", str(e))
        raise
    finally:
        if 'connection' in locals():
            connection.close()


def process_extra_columns(spark, correct_files):
    """Process extra columns in files"""
    logger.info("Processing extra columns in files...")
    try:
        final_df_to_process = final_df()
        final_df_to_process.show()

        for data in correct_files:
            data_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(data)
            data_schema = data_df.columns
            extra_columns = list(set(data_schema) - set(config.mandatory_columns))
            logger.debug("Extra columns in file '%s': %s", data, extra_columns)

            if extra_columns:
                data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
                    .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                            "price", "quantity", "total_cost", "additional_column")
                logger.info("Processed file '%s' and added 'additional_column'.", data)
            else:
                data_df = data_df.withColumn("additional_column", lit(None)) \
                    .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                            "price", "quantity", "total_cost", "additional_column")

            final_df_to_process = final_df_to_process.union(data_df)

        logger.info("Final DataFrame ready for processing.")
        final_df_to_process.show()
        final_df_to_process.cache()
        return final_df_to_process
    except Exception as e:
        logger.error("Failed to process extra columns: %s", str(e))
        raise


def enrich_data(final_df_to_process):
    """Enrich data with dimension tables"""
    try:
        logger.info("Enriching data with dimension tables...")
        customer_table_df = customer_table()
        product_table_df = product_table()
        product_staging_table_df = product_staging_table()
        sales_team_table_df = sales_team_table()
        store_table_df = store_table()

        s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                               customer_table_df,
                                                               store_table_df,
                                                               sales_team_table_df)

        logger.info("Data enrichment completed successfully.")
        s3_customer_store_sales_df_join.show()
        return s3_customer_store_sales_df_join
    except Exception as e:
        logger.error("Failed to enrich data: %s", str(e))
        raise


def process_customer_mart(s3_client, s3_customer_store_sales_df_join):
    """Process customer data mart"""
    logger.info("Processing customer data mart...")
    try:
        final_customer_data_mart_df = s3_customer_store_sales_df_join \
            .select("ct.customer_id",
                    "ct.first_name", "ct.last_name", "ct.address",
                    "ct.pincode", "phone_number",
                    "sales_date", "total_cost")
        logger.info("Final customer data mart DataFrame:")
        final_customer_data_mart_df.show()

        parquet_writer = FileWriter("overwrite", "parquet")
        parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)
        logger.info("Customer data written to local disk: %s", config.customer_data_mart_local_file)

        s3_uploader = UploadToS3(s3_client)
        s3_directory = config.s3_customer_datamart_directory
        message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
        logger.info("Customer data uploaded to S3: %s", message)

        final_customer_data_mart_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .bucketBy(10, "customer_id") \
            .option("path", config.process_customer_mart_bucketed_local_file) \
            .saveAsTable("customer_data_mart_bucketed")
        logger.info("Bucketed data saved to: %s", config.process_customer_mart_bucketed_local_file)

        s3_prefix = "customer_bucketed_data_mart"
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        for root, dirs, files in os.walk(config.process_customer_mart_bucketed_local_file):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(local_file_path,
                                                     config.process_customer_mart_bucketed_local_file)
                s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
                s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

        return final_customer_data_mart_df
    except Exception as e:
        logger.error("Failed to process customer data mart: %s", str(e))
        raise


def process_sales_mart(s3_client, s3_customer_store_sales_df_join):
    """Process sales team data mart"""
    logger.info("Processing sales team data mart...")
    try:
        s3_uploader = UploadToS3(s3_client)

        final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
            .select("store_id",
                    "sales_person_id", "sales_person_first_name", "sales_person_last_name",
                    "store_manager_name", "manager_id", "is_manager",
                    "sales_person_address", "sales_person_pincode",
                    "sales_date", "total_cost", expr("SUBSTRING(sales_date,1,7) as sales_month"))

        logger.info("Final sales team data mart DataFrame:")
        final_sales_team_data_mart_df.show()

        parquet_writer = FileWriter("overwrite", "parquet")
        parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
        logger.info("Sales team data written to local disk: %s", config.sales_team_data_mart_local_file)

        s3_directory = config.s3_sales_datamart_directory
        message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
        logger.info("Sales team data uploaded to S3: %s", message)

        final_sales_team_data_mart_df.write.format("parquet") \
            .option("header", "true") \
            .mode("overwrite") \
            .partitionBy("sales_month", "store_id") \
            .option("path", config.sales_team_data_mart_partitioned_local_file) \
            .save()

        s3_prefix = "sales_partitioned_data_mart"
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
                s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
                s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

        logger.info("Sales team data partitioned and uploaded to S3.")
        return final_sales_team_data_mart_df
    except Exception as e:
        logger.error("Failed to process sales team data mart: %s", str(e))
        raise


def calculate_marts(final_customer_data_mart_df, final_sales_team_data_mart_df):
    """Calculate and write mart calculations"""
    try:
        logger.info("Calculating customer mart...")
        customer_mart_calculation_table_write(final_customer_data_mart_df)

        logger.info("Calculating sales mart...")
        sales_mart_calculation_table_write(final_sales_team_data_mart_df)

    except Exception as e:
        logger.error("Failed to calculate marts: %s", str(e))
        raise


def cleanup_files(s3_client, correct_files):
    """Cleanup files and update staging table"""
    try:
        source_prefix = config.s3_source_directory
        destination_prefix = config.s3_processed_directory
        message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
        logger.info("Files moved to processed directory: %s", message)

        logger.info("Cleaning up local files...")
        delete_local_file(config.local_directory)
        delete_local_file(config.customer_data_mart_local_file)
        delete_local_file(config.sales_team_data_mart_local_file)
        delete_local_file(config.sales_team_data_mart_partitioned_local_file)
        logger.info("Local files cleaned up successfully.")

        update_statements = []
        db_name = config.database_name
        current_date = datetime.datetime.now()
        formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

        if correct_files:
            for file in correct_files:
                filename = os.path.basename(file)
                statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                             f"SET status = 'I',updated_date='{formatted_date}' " \
                             f"WHERE file_name = '{filename}'"
                update_statements.append(statements)

            logger.debug("Generated update statements: %s", update_statements)
            connection = get_mysql_connection()
            cursor = connection.cursor()

            for statement in update_statements:
                cursor.execute(statement)
                connection.commit()
            cursor.close()
            connection.close()
            logger.info("Staging table updated successfully.")
        else:
            logger.error("No valid files to update in staging table.")
            raise Exception("Process failed.")
    except Exception as e:
        logger.error("Failed to clean up files: %s", str(e))
        raise
    finally:
        if 'connection' in locals():
            connection.close()


def main():
    """Main execution function"""
    try:
        s3_client = setup_s3_client()
        check_local_files()
        s3_absolute_file_path = list_s3_files(s3_client)
        download_s3_files(s3_client, s3_absolute_file_path)
        csv_files, error_files = process_local_files()
        spark = create_spark_session()
        correct_files, error_files = check_file_schemas(spark, csv_files)
        move_error_files(s3_client, error_files)
        update_staging_table_start(correct_files)
        final_df_to_process = process_extra_columns(spark, correct_files)
        s3_customer_store_sales_df_join = enrich_data(final_df_to_process)
        final_customer_data_mart_df = process_customer_mart(s3_client, s3_customer_store_sales_df_join)
        final_sales_team_data_mart_df = process_sales_mart(s3_client, s3_customer_store_sales_df_join)
        calculate_marts(final_customer_data_mart_df, final_sales_team_data_mart_df)
        cleanup_files(s3_client, correct_files)

        input("Press enter to terminate ")
    except Exception as e:
        logger.error("Main process failed: %s", str(e))
        raise


if __name__ == "__main__":
    main()