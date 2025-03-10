from pyspark.sql.functions import *
from src.main.utility.logging_config import *
#enriching the data from different table
def dimesions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df):


    # WR = this is only selected columns not all columns
    logger.info("Joining the final_df_to_process with customer_table_df ")
    s3_customer_df_join = final_df_to_process.alias("s3_data")\
                          .join(customer_table_df.alias("ct"),col("s3_data.customer_id") == col("ct.customer_id"),"inner")\
        .select(
            col("store_id"),
            col("sales_date"),
            col("sales_person_id"),
            col("total_cost"),
            col("ct.customer_id"),
            col("first_name"),
            col("last_name"),
            col("address"),
            col("pincode"),
            col("phone_number")
        )


    # s3_customer_df_join.printSchema()



    logger.info("Joining the s3_customer_df_join with store_table_df ")

    # WR = this is only selected columns not all columns

    s3_customer_store_df_join = s3_customer_df_join.join(store_table_df,store_table_df["id"] == s3_customer_df_join["store_id"],"inner")\
        .select(
            col("store_id"),
            col("sales_date"),
            col("sales_person_id"),
            col("total_cost"),
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("ct.address"),
            col("pincode"),
            col("phone_number"),
            col("store_manager_name")
        )


    #step 3 where i am adding sales team table details
    # s3_customer_store_df_join.join(sales_team_table_df,
    #                          sales_team_table_df["id"]==s3_customer_store_df_join["sales_person_id"],
    #                          "inner").show()


    #But i do not need all the columns so dropping it
    #save the result into s3_customer_store_sales_df_join
    logger.info("Joining the s3_customer_store_df_join with sales_team_table_df ")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                                                                     col("st.id") == s3_customer_store_df_join[
                                                                         "sales_person_id"],
                                                                     "inner") \
        .withColumn("sales_person_first_name", col("st.first_name")) \
        .withColumn("sales_person_last_name", col("st.last_name")) \
        .withColumn("sales_person_address", col("st.address")) \
        .withColumn("sales_person_pincode", col("st.pincode")) \
        .drop("id", "st.first_name", "st.last_name", "st.address", "st.pincode")

    return s3_customer_store_sales_df_join
