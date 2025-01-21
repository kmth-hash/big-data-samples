# Run the following in Google-colab notebook 

# Install packages 
!pip install pyspark==3.4.0
!wget -q https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.4/spark-snowflake_2.12-2.12.0-spark_3.4.jar
!wget -q https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.30/snowflake-jdbc-3.13.30.jar

# Verify installation 
!pyspark --version 
import pyspark
import random
import os 
print(pyspark.__version__)

# Import pyspark packages 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf

# Add snowflake jar package in spark config parameters
spark = SparkSession.builder \
    .appName("SnowflakeDataRead") \
    .config("spark.jars", "/content/spark-snowflake_2.12-2.12.0-spark_3.4.jar,/content/snowflake-jdbc-3.13.30.jar") \
    .getOrCreate()

# Snowflake options 

sfOptions = {
    "sfURL" : "https://ab10000.ap-southeast-1.snowflakecomputing.com", # connection URL 
    "sfAccount" : "xy10000", #sfAccount from login window --> https://<sfAccount>.ap-southeast-1.snowflakecomputing.com/oauth/authorize?
    "sfUser" : "user",
    "sfPassword" : "password",
    "sfDatabase" : "database",
    "sfSchema" : "PUBLIC",
    "sfWarehouse" : "COMPUTE_WH",
    "sfRole" : "ACCOUNTADMIN",
    "sfDriver" : "net.snowflake.client.jdbc.SnowflakeDriver",
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
snowflake_src_table = "tableName"
snowflake_dest_table = "classReg" 

df = spark.read \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("query", f"select * from {snowflake_src_table}") \
    .load()
# Show the data (for testing purposes)
df.show()
print("Connection Established Successfully!")

# Creating a temp dataframe to load into snowflake table 
grades = ['A+' , 'A' , 'A-', 'B+' , 'B' , 'C' , 'D' , 'E' , 'F']

df = [ (i+1, random.choice(grades)) for i in range(124)]
columns = ['rollno','grades']

print(columns)
print(df)

df = spark.createDataFrame(data=df, schema=columns) 
df.show()

df.write\
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", snowflake_dest_table) \
    .options(header=True) \
    .mode("append") \
    .save()
print("Data loaded into Snowflake successfully! ")
