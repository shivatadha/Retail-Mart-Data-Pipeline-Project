# Retail Mart Data Pipeline Project

## About the Project 🚀

Welcome to the **Retail Mart Data Pipeline** project! This project is designed to efficiently process, transform, and analyze retail data using a robust ETL (Extract, Transform, Load) pipeline. By leveraging Apache Spark for big data processing, AWS S3 for cloud storage, and MySQL for structured data management, this solution ensures data integrity, scalability, and high performance. Whether you're a data engineer, analyst, or simply curious about data pipelines, this project provides an end-to-end solution for retail analytics.





## Workflow of the Project 📊

The Retail Mart Data Pipeline handles the complete lifecycle of retail data. Here’s a detailed breakdown of its workflow:

![Retail Mart Data Pipeline Architecture](https://github.com/shivatadha/Retail-Mart-Data-Pipeline-Project/blob/9913ce2fd505fa96fe0dbfbb4a7245e9b86cdcdb/Retail%20Mart%20Data%20Pipeline%20Project.png)



---

1. **Setup S3 Client** 🚀  
   - **Initialize S3 Client:** Configures an S3 client using AWS credentials to list, download, and manage files within AWS S3 buckets.

2. **Check Local Files** 📂  
   - **Local Verification:** Checks the local directory for any pre-existing files and validates their status against the staging table to avoid duplicates and reprocessing of failed files.

3. **List S3 Files** 📄  
   - **File Discovery:** Lists available files in the designated S3 bucket and folder. If no files are found, the process is halted to prevent unnecessary processing.

4. **Download S3 Files** 📥  
   - **File Transfer:** Downloads identified files from S3 to the local directory, making them available for further processing.

5. **Process Local Files** 📋  
   - **File Filtering:** Differentiates valid CSV files from other files, ensuring only correct files proceed while error files are flagged.

6. **Create Spark Session** 🔥  
   - **Spark Initialization:** Launches an Apache Spark session for efficient, distributed processing of large datasets.

7. **Check File Schemas** 📐  
   - **Schema Validation:** Validates each CSV file against mandatory columns. Files missing required columns are flagged and handled as errors.

8. **Move Error Files** 🚫  
   - **Error Management:** Moves files with schema issues to a designated error directory for further analysis and troubleshooting.

9. **Update Staging Table** 📝  
   - **Staging Update:** Inserts records into the staging table to mark files as active ('A') for processing, ensuring traceability.

10. **Process Extra Columns** 📝  
    - **Data Enrichment:** Processes extra columns in the CSV files by consolidating them into a single field (`additional_column`), ensuring a uniform data structure.

11. **Enrich Data** 🌟  
    - **Data Joins:** Enriches the dataset by joining it with various dimension tables (e.g., customer, product, store), creating a comprehensive view of the retail data.

12. **Process Customer Mart** 🛒  
    - **Customer Data Mart:** Creates a customer data mart by selecting and transforming enriched data. The data is then written locally, uploaded to S3, and organized in a bucketed format.

13. **Process Sales Mart** 📈  
    - **Sales Data Mart:** Similarly, processes sales data into a dedicated data mart, partitioning and uploading the results to S3 for optimized querying.

14. **Calculate Marts** 🧮  
    - **Final Calculations:** Performs final calculations for both customer and sales data marts, generating key metrics for analysis.

15. **Cleanup Files** 🧹  
    - **Housekeeping:** Cleans up local files and updates the staging table to mark processed files as inactive ('I'), ensuring a fresh state for subsequent runs.

---

## Features 🌟

- **Robust Error Handling:** Comprehensive logging and error management to catch and resolve issues promptly.
- **Schema Validation:** Ensures data integrity by verifying the structure of incoming files.
- **Data Enrichment:** Joins data with dimension tables to create a rich, unified dataset.
- **Scalable Processing:** Utilizes Apache Spark for distributed processing of large datasets.
- **Data Mart Creation:** Constructs both customer and sales data marts for streamlined analytics.
- **Automated Cleanup:** Manages file housekeeping and staging updates automatically.
- **Modular Architecture:** Each functional aspect of the pipeline is separated into distinct, maintainable modules.

---

## 🛠️ Module Breakdown

1. **Core Orchestrator (`main.py`)**  
   - **Workflow Manager:** Manages the end-to-end ETL workflow and coordinates module interactions.  
   - **Checkpointing:** Implements checkpoints and error logging to monitor process flow and recover from failures.

2. **Data Movers (`move/`)**  
   - **S3 Operations:** Handles S3-to-S3 file transfers, error file relocation, and movement of files between different S3 directories.  
   - **Local Cleanup:** Manages the deletion and cleanup of local files post-processing.

3. **Spark Processors (`transformations/jobs/`)**  
   - **Schema Validator:** Checks and validates the schema of CSV files to ensure all mandatory columns are present.  
   - **Data Enricher:** Processes extra columns and enriches data by joining it with dimension tables (customer, product, store).  
   - **Mart Calculator:** Applies business logic and window functions to compute key metrics for customer and sales data marts.

4. **Storage Handlers (`upload/` and `download/`)**  
   - **File Downloader:** Manages the downloading of raw data files from AWS S3 to local directories.  
   - **Parquet Writer:** Implements efficient writing of processed data into Parquet format with partitioning and compression (e.g., Snappy).  
   - **S3 Uploader:** Uploads processed data back to AWS S3, organizing files into bucketed or partitioned formats for optimized querying.

5. **Operational Modules (`utility/`)**  
   - **Credential Manager:** Securely manages and decrypts AWS and MySQL credentials.  
   - **Spark Session Factory:** Creates and configures Spark sessions tailored for distributed data processing.  
   - **Database Connector:** Sets up and manages MySQL connection pools for efficient staging and final data storage.  
   - **Logging Configurator:** Implements logging configurations for monitoring, debugging, and performance tracking.

---

## Project Structure 📂

```
my_project/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   ├── delete/
│   │   │   ├── aws_delete.py
│   │   │   └── local_file_delete.py
│   │   ├── download/
│   │   │   └── aws_file_download.py
│   │   ├── move/
│   │   │   └── move_files.py
│   │   ├── read/
│   │   │   ├── aws_read.py
│   │   │   └── database_read.py
│   │   ├── transformations/
│   │   │   └── jobs/
│   │   │       ├── customer_mart_sql_transform_write.py
│   │   │       ├── dimension_tables_join.py
│   │   │       ├── main.py
│   │   │       └── sales_mart_sql_transform_write.py
│   │   ├── upload/
│   │   │   └── upload_to_s3.py
│   │   ├── utility/
│   │   │   ├── dataframe.py
│   │   │   ├── encrypt_decrypt.py
│   │   │   ├── logging_config.py
│   │   │   ├── s3_client_object.py
│   │   │   ├── spark_session.py
│   │   │   └── my_sql_session.py
│   │   └── write/
│   │       ├── database_write.py
│   │       └── parquet_write.py
│   ├── test/
│   │   ├── generate_csv_data.py
│   │   ├── sales_data_upload_s3.py
│   │   └── test.py
```

---

## Tech Stack 🛠️

| **Technology**         | **Purpose**                                                  |
|------------------------|--------------------------------------------------------------|
| **Python**             | Core programming language for the pipeline                 |
| **Apache Spark**       | Distributed data processing and transformation (PySpark)   |
| **AWS S3**             | Cloud storage for raw and processed data                     |
| **MySQL**              | Relational database for staging and final data storage       |
| **Pandas**             | Data manipulation and analysis (if needed)                   |
| **Logging (Python)**   | Logging and monitoring of the ETL process                    |
| **Jupyter Notebooks**  | Data exploration, visualization, and prototyping             |

---

## Installation and Setup 🛠️

Follow these steps to set up and run the project locally:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/retail-mart-data-pipeline.git
   cd retail-mart-data-pipeline
   ```

2. **Create a Virtual Environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # For Linux/Mac
   venv\Scripts\activate     # For Windows
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure AWS Credentials:**
   - Install and configure the AWS CLI with the necessary permissions.
   - Update your configuration file (`config.py`, `config.yaml`, etc.) with your AWS access key and secret key.

5. **Set Up MySQL Database:**
   - Create a MySQL database and update the configuration file with your database connection details.
   - Execute any provided SQL scripts to create the required tables.

6. **Run the Project:**
   ```bash
   python src/main/transformations/jobs/main.py
   ```
   <sup>*(Adjust the path to your main script if needed.)*</sup>

---

## Future Improvements 🚀

- **Enhanced Error Handling:** Develop more sophisticated error recovery and alerting mechanisms.
- **Dynamic Configuration:** Implement dynamic configuration updates without needing to restart the pipeline.
- **Real-Time Data Processing:** Integrate streaming platforms like Apache Kafka for real-time data ingestion.
- **Performance Optimization:** Explore advanced optimizations with Apache Hudi or Delta Lake for incremental processing.
- **Containerization:** Containerize the application using Docker for easier deployment and scalability.
- **Monitoring & Alerts:** Integrate tools like Prometheus and Grafana for real-time performance monitoring.

---

## Conclusion 🎉

The **Retail Mart Data Pipeline** project offers a comprehensive and scalable solution for processing and analyzing retail data. With a modular architecture and robust ETL workflow, this pipeline ensures that data is ingested, transformed, and loaded efficiently for advanced analytics. Whether you're building a production data pipeline or exploring retail analytics, this project serves as an excellent starting point.
