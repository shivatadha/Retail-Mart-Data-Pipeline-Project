import os
from datetime import date, datetime
from decimal import Decimal


from pyspark.sql.functions import *
from pyspark.sql.types import *
os.environ["PYSPARK_PYTHON"] = "E:\\pycharm\\Shop_project\\shopproject.venv\\Scripts\\python.exe"
from pyspark.python.pyspark.shell import spark

def final_df():

    final_df_schema = StructType([
                                   StructField("customer_id", IntegerType(), True),
                                   StructField("store_id", IntegerType(), True),
                                   StructField("product_name", StringType(), True),
                                   StructField("sales_date", DateType(), True),
                                   StructField("sales_person_id", IntegerType(), True),
                                   StructField("price", FloatType(), True),
                                   StructField("quantity", IntegerType(), True),
                                   StructField("total_cost", FloatType(), True),
                                   StructField("additional_column", StringType(), True)
                                ])
    final_df_schema_to_process = spark.createDataFrame([], final_df_schema)
    return final_df_schema_to_process

#----------------------------- customer_table -----------------------------------#

def customer_table():
    # Define the schema
    customer_table_name_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("pincode", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("customer_joining_date", DateType(), True)
    ])


    customer_table_data = [
        (1, 'Saanvi', 'Krishna', 'Delhi', '122009', '9173121081', date(2021, 1, 20)),
        (2, 'Dhanush', 'Sahni', 'Delhi', '122009', '9155328165', date(2022, 3, 27)),
        (3, 'Yasmin', 'Shan', 'Delhi', '122009', '9191478300', date(2023, 4, 8)),
        (4, 'Vidur', 'Mammen', 'Delhi', '122009', '9119017511', date(2020, 10, 12)),
        (5, 'Shamik', 'Doctor', 'Delhi', '122009', '9105180499', date(2022, 10, 30)),
        (6, 'Ryan', 'Dugar', 'Delhi', '122009', '9142616565', date(2020, 8, 10)),
        (7, 'Romil', 'Shanker', 'Delhi', '122009', '9129451313', date(2021, 10, 29)),
        (8, 'Krish', 'Tandon', 'Delhi', '122009', '9145683399', date(2020, 1, 8)),
        (9, 'Divij', 'Garde', 'Delhi', '122009', '9141984713', date(2020, 11, 10)),
        (10, 'Hunar', 'Tank', 'Delhi', '122009', '9169808085', date(2023, 1, 27)),
        (11, 'Zara', 'Dhaliwal', 'Delhi', '122009', '9129776379', date(2023, 6, 13)),
        (12, 'Sumer', 'Mangal', 'Delhi', '122009', '9138607933', date(2020, 5, 1)),
        (13, 'Rhea', 'Chander', 'Delhi', '122009', '9103434731', date(2023, 8, 9)),
        (14, 'Yuvaan', 'Bawa', 'Delhi', '122009', '9162077019', date(2023, 2, 18)),
        (15, 'Sahil', 'Sabharwal', 'Delhi', '122009', '9174928780', date(2021, 3, 16)),
        (16, 'Tiya', 'Kashyap', 'Delhi', '122009', '9105126094', date(2023, 3, 23)),
        (17, 'Kimaya', 'Lala', 'Delhi', '122009', '9115616831', date(2021, 3, 14)),
        (18, 'Vardaniya', 'Jani', 'Delhi', '122009', '9125068977', date(2022, 7, 19)),
        (19, 'Indranil', 'Dutta', 'Delhi', '122009', '9120667755', date(2023, 7, 18)),
        (20, 'Kavya', 'Sachar', 'Delhi', '122009', '9157628717', date(2022, 5, 4)),
        (21, 'Manjari', 'Sule', 'Delhi', '122009', '9112525501', date(2023, 2, 12)),
        (22, 'Akarsh', 'Kalla', 'Delhi', '122009', '9113226332', date(2021, 3, 5)),
        (23, 'Miraya', 'Soman', 'Delhi', '122009', '9111455455', date(2023, 7, 6)),
        (24, 'Shalv', 'Chaudhary', 'Delhi', '122009', '9158099495', date(2021, 3, 14)),
        (25, 'Jhanvi', 'Bava', 'Delhi', '122009', '9110074097', date(2022, 7, 14))
    ]

    return spark.createDataFrame(customer_table_data, schema=customer_table_name_schema)


def product_staging_table():
        product_schema = StructType([
            StructField("id", IntegerType(), True),  # Auto-increment is not directly handled in PySpark DataFrames
            StructField("file_name", StringType(), True),
            StructField("file_location", StringType(), True),
            StructField("created_date", TimestampType(), True),
            StructField("updated_date", TimestampType(), True),
            StructField("status", StringType(), True)
        ])
        return spark.createDataFrame([], schema=product_schema)
#---------------------- store_schema --------------------------------
def store_table():
        store_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("store_pincode", StringType(), True),
            StructField("store_manager_name", StringType(), True),
            StructField("store_opening_date", DateType(), True),
            StructField("reviews", StringType(), True)
        ])

        store_data = [
            (121, 'Delhi', '122009', 'Manish', date(2022, 1, 15), 'Great store with a friendly staff.'),
            (122, 'Delhi', '110011', 'Nikita', date(2021, 8, 10), 'Excellent selection of products.'),
            (123, 'Delhi', '201301', 'Vikash', date(2023, 1, 20), 'Clean and organized store.'),
            (124, 'Delhi', '400001', 'Rakesh', date(2020, 5, 5), 'Good prices and helpful staff.')
        ]

        return spark.createDataFrame(store_data, schema=store_schema)
# Product Table
def product_table():
        product_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("current_price", DecimalType(10, 2), True),
            StructField("old_price", DecimalType(10, 2), True),
            StructField("created_date", TimestampType(), True),
            StructField("updated_date", TimestampType(), True),
            StructField("expiry_date", DateType(), True)
        ])

        product_data = [
            (1, 'quaker oats', Decimal(212), Decimal(212), datetime(2022, 5, 15, 0, 0), None, date(2025, 1, 1)),
            (2, 'sugar', Decimal(50), Decimal(50), datetime(2021, 8, 10, 0, 0), None, date(2025, 1, 1)),
            (3, 'maida', Decimal(20), Decimal(20), datetime(2023, 3, 20, 0, 0), None, date(2025, 1, 1)),
            (4, 'besan', Decimal(52), Decimal(52), datetime(2020, 5, 5, 0, 0), None, date(2025, 1, 1)),
            (5, 'refined oil', Decimal(110), Decimal(110), datetime(2022, 1, 15, 0, 0), None, date(2025, 1, 1)),
            (6, 'clinic plus', Decimal(1.5), Decimal(1.5), datetime(2021, 9, 25, 0, 0), None, date(2025, 1, 1)),
            (7, 'dantkanti', Decimal(100), Decimal(100), datetime(2023, 7, 10, 0, 0), None, date(2025, 1, 1)),
            (8, 'nutrella', Decimal(40), Decimal(40), datetime(2020, 11, 30, 0, 0), None, date(2025, 1, 1))

        ]

        return  spark.createDataFrame(product_data, schema=product_schema)

# Sales Team Table
def sales_team_table():
        sales_team_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("manager_id", IntegerType(), True),
            StructField("is_manager", StringType(), True),
            StructField("address", StringType(), True),
            StructField("pincode", StringType(), True),
            StructField("joining_date", DateType(), True)
        ])

        sales_team_data = [
            (1, 'Rahul', 'Verma', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (2, 'Priya', 'Singh', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (3, 'Amit', 'Sharma', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (4, 'Sneha', 'Gupta', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (5, 'Neha', 'Kumar', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (6, 'Vijay', 'Yadav', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (7, 'Anita', 'Malhotra', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (8, 'Alok', 'Rajput', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (9, 'Monica', 'Jain', 10, 'N', 'Delhi', '122007', date(2020, 5, 1)),
            (10, 'Rajesh', 'Gupta', 10, 'Y', 'Delhi', '122007', date(2020, 5, 1))
        ]

        return spark.createDataFrame(sales_team_data, schema=sales_team_schema)

# S3 Bucket Info Table
def s3_bucket():
        s3_bucket_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("bucket_name", StringType(), True),
            StructField("file_location", StringType(), True),
            StructField("created_date", TimestampType(), True),
            StructField("updated_date", TimestampType(), True),
            StructField("status", StringType(), True)
        ])

        s3_bucket_data = [
            (1, 'youtube-project-testing', 's3://youtube-project-testing', datetime(2023, 7, 1, 0, 0), datetime(2023, 7, 1, 0, 0), 'active')
        ]

        return spark.createDataFrame(s3_bucket_data, schema=s3_bucket_schema)

# Customers Data Mart Table
def customers_data_mart():
        customers_data_mart_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("full_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("sales_date_month", DateType(), True),
            StructField("total_sales", DecimalType(10, 2), True)
        ])


        return spark.createDataFrame([],schema=customers_data_mart_schema)

# Sales Team Data Mart Table
def sales_team_data_mart():
        sales_team_data_mart_schema = StructType([
            StructField("store_id", IntegerType(), True),
            StructField("sales_person_id", IntegerType(), True),
            StructField("full_name", StringType(), True),
            StructField("sales_month", StringType(), True),
            StructField("total_sales", DecimalType(10, 2), True),
            StructField("incentive", DecimalType(10, 2), True)
        ])
        return spark.createDataFrame([], schema=sales_team_data_mart_schema)

