import os

key = ""
iv = ""
salt = ""

# AWS Access And Secret key
aws_access_key = ""
aws_secret_key = ""
bucket_name = "grocery-shop-project"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"

#Database credential
# MySQL database connection properties
database_name = "shop_project"
url = f"jdbc:mysql://localhost:3306/shop_project"
properties = {
    "user": "root",
    "password": "12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                     "total_cost"]

# File Download location
local_directory = "E:\\pycharm\\data-engineer\\resource\\data\\file_from_s3\\"
sales_data_to_s3 = "E:\\pycharm\\data-engineer\\resource\\data\\sales_data_to_s3\\"
customer_data_mart_local_file = "E:\\pycharm\\data-engineer\\resource\\data\\customer_data_mart\\"
sales_team_data_mart_local_file = "E:\\pycharm\\data-engineer\\resource\\data\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "E:\\pycharm\\data-engineer\\resource\\data\\sales_partition_data\\"
process_customer_mart_bucketed_local_file = "E:\\pycharm\\data-engineer\\resource\\data\\customer_bucket_data\\"
error_folder_path_local = "E:\\pycharm\\data-engineer\\resource\\data\\error_files\\"
