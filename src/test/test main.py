import os

from src.main.Write.file_writer import FileWriter
from src.main.transformation.job.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformation.job.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.my_sql_session import get_mysql_connection

# import datetime
os.environ["PYSPARK_PYTHON"] = "E:\\pycharm\\Shop_project\\shopproject.venv\\Scripts\\python.exe"
from resource.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.Read.aws_read import *
from src.main.utility.spark_session import *
from src.main.utility.dataframe import *
from datetime import datetime
from src.main.transformation.job.dimension_tables_join import dimesions_table_join


#-------------------------------------------------------- GET S3 CLIENT------------------------------------------


aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response =s3_client.list_buckets()
logger.info("list of Buckets :%s",response['Buckets'])

#--------------------------------------------------- MYSQL CONNECT -------------------------------------------------#

# check if local directory has already a file
# if file is there then check if the same file is present in staging area
# with status as A. if so then don't delete and try to re-run
# else give an error and not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
print(csv_files)
connection = get_mysql_connection()
cursor = connection.cursor()
total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    statement = f"""
    select distinct file_name from
    shop_project.product_staging_table
    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
    """
    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No Record Match")
else:
    logger.info('Last run was successfull!!!')


# ################------------------------------- Read Data From S3 ---------------------------------################


try:
    s3_reader = s3Reader()

    #Bucket name should come from table

    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path = folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No Data Available to process")
except Exception as e:
    logger.error("Exited with error:- %s", e)
    raise e

#'s3://grocery-shop-project/sales_data/sales_data.csv'


# #---------------------------------- Download Data From S3 -----------------------------------------------------------#

bucket_name = config.bucket_name
local_directory = config.local_directory
prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info("File path available on s3 under %s bucket and folder name is %s ", bucket_name, file_paths)
try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("file download error: %s", e)
    sys.exit()

#---------------- --------------- Get a list of all files in the local directory -------------------------------------#

all_files = os.listdir(config.local_directory)
logger.info(f"List al the files present on my local directory after download: {all_files}")
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
    if not csv_files:
        logger.error("No data available to process the request")
        raise Exception("No data available to process the request")
    logger.info("All csv File is :- %s", csv_files)
    logger.info("All error File is :- %s ", error_files)
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")


#-------------------------------------------------------------------------------------------------------------------------------------

logger.info("**********************Creating Spark Session*************************")
spark = spark_session()
logger.info("**********************Spark Session Created*************************")



#---------------------------------- Checking schema for data loaded in S3 ---------------------------------------------#

#Check required columns in schema of csv files
#If not required column keep it in list of error files
#Else union data in one dataframe

logger.info("******************Checking schema for data loaded in S3*********************")
corrected_files = []
for data in csv_files:
    data_schema = spark.read.format("csv") \
        .option("header", "true") \
        .load(data).columns
    print(data)
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns in schema are {config.mandatory_columns}")

    missing_columns = set(config.mandatory_columns) - set(data_schema)

    logger.info("Missing columns are: %s", missing_columns)

    if missing_columns:
        error_files.append(data)
    else:
        logger.info("No missing columns for: %s", data)
        corrected_files.append(data)

logger.info(f"**************List of corrected file: {corrected_files}****************")
logger.info(f"**************List of error file: {error_files}****************")
logger.info("Processing error files to error directory if any********")


#-------------------------------- Additional columns needs to be taken care of ---------------------------------------#


#Determine extra columns
#Before running process
#Stage table needs to be updated with status as Active(A) or Inactive(I)


logger.info("********Updating the product_staging_table that we hava started the process")
insert_statement = []
db_name = config.database_name
current_date = datetime.now()
formated_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if corrected_files:
    for file in corrected_files:
        filename = os.path.basename(file)
        statement = f"INSERT INTO {db_name}.{config.product_staging_table}" \
                    f"(file_name, file_location, created_date, status)" \
                    f" VALUES('{filename}','{file}','{formated_date}','A')"
        insert_statement.append(statement)
    logger.info("Insert statement created for staging table %s", insert_statement)
    logger.info("**********Connecting to mysql server**********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("******** Successfully connected to MYSQL server")

    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
else:
    logger.error("********There are no files to process********")
    raise Exception("*********No data available with corrected files")
logger.info("Staging table updated successfully")

#----------------------------------- Fixing extra Columns comming from source ------------------------------------------

logger.info("********Fixing extra Columns comming from source")
# making data dataframe for
final_df_to_process = final_df_to_process()
final_df_to_process.show()

#Create new column with concatenated values of extra column

for data in corrected_files:
    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data)
    data_schema = data_df.columns

    extra_columns = list(set(data_schema) - set(config.mandatory_columns))

    logger.info("Extra columns in corrected files are: %s", extra_columns)
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_column")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_column")
    final_df_to_process = final_df_to_process.union(data_df)

logger.info("This is the final df which will be process further")

final_df_to_process.show(5)

# store_df = dataframe.store_df


#Enrich the data from all dimension table
#also create a datamart for sales_team and their incentive, address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that
#there should be a store_id segrigation
#Read the data from parquet and generate a csv file
#in which there will be a sales_person_name, sales_person_store_id
#sales_person_total_billing_done_for_each_month, total_incentive


# customer_table_name = "customer"
# product_staging_table = "product_staging_table"
# product_table = "product"
# sales_team_table = "sales_team"
# store_table = "store"

logger.info("Loading customer data into customer_table_df")
customer_table_df = customer_table_df()
customer_table_df.show(5)

logger.info("Loading product_table data into product_table_df")
product_table_df = product_table_df()
product_table_df.show(5)

logger.info("Loading sales_team_table data into sales_team_table_df")
sales_team_table_df = sales_team_table_df()
sales_team_table_df.show(5)

logger.info("Loading store_table data into store_table_df")
store_table_df = store_table_df()
store_table_df.show(5)

logger.info("Loading product_staging_table data into product_staging_table_df")
product_staging_table_df = product_staging_table_df()


#--n----------------------------- Dimension Table Join Transformation ----------------------------------------------------------

S3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)

#Final enriched data
logger.info("************ Final Enriched Data ********************")
S3_customer_store_sales_df_join.show(5)



#Write the customer data into customer data mart in parquet format
#file will be written to local first
#move the RAW data to s3 bucket for reporting tool
#Write reporting data into MySQL table also


logger.info("*************** write the data into Customer Data Mart **********")
final_customer_data_mart_df = S3_customer_store_sales_df_join.select("ct.customer_id",
                            "ct.first_name","ct.last_name","ct.address","ct.pincode",
                            "phone_number","sales_date","total_cost")

logger.info("*************** Final Data for customer Data Mart **********")
final_customer_data_mart_df.show(5)

logger.info(f"*************** customer data written to local disk at {config.customer_data_mart_local_file} **********")

#---------------------- parquet_writer ---------------------------------#
parquet_writer = FileWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)




#Move data on s3 bucket for customer_data_mart
logger.info(f"*************** Data Movement from local to s3 for customer data mart **********")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message  = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")


#sales_team Data Mart
logger.info("*************** write the data into sales team Data Mart **********")
final_sales_team_data_mart_df = S3_customer_store_sales_df_join\
            .select("store_id",
                    "sales_person_id","sales_person_first_name" ,"sales_person_last_name",
                    "store_manager_name","manager_id","is_manager",
                    "sales_person_address","sales_person_pincode"
                    ,"sales_date","total_cost", expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("*************** Final Data for sales team Data Mart **********")
final_sales_team_data_mart_df.show(5)

logger.info(f"*************** sales team data written to local disk at {config.sales_team_data_mart_local_file} **********")
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)


#Move data on s3 bucket for sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message  = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")


#Also writing the data into partitions
parquet_writer_by_partition = FileWriter("overwrite","parquet")
parquet_writer_by_partition.dataframe_writer_partitionBy(final_sales_team_data_mart_df,config.sales_team_data_mart_partitioned_local_file)


#Move data on s3 for partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table

logger.info("******Calculating customer every month purchased amount *******")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("******Calculation of customer mart done and written into the table*********")


#calculation for sales team mart
#find out the total sales done by each sales person every month
#Give the top performer 1% incentive of total sales of the month
#Rest sales person will get nothing
#write the data into MySQL table
logger.info("******Calculating sales every month billed amount *******")

sales_mart_calculation_table_write(final_sales_team_data_mart_df)

logger.info("******Calculation of sales mart done and written into the table*********")