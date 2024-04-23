import os
import sys
import findspark
import os
import sys
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/Users/hoon/Downloads/spark-3.5.1-bin-hadoop3"

import pyspark
import findspark
from pyspark.sql import SparkSession

# Initialize findspark and PySpark environment
findspark.init()

# Set up MongoDB connection parameters for Spark
os.environ["SPARK_HOME"] = "/Users/hoon/Downloads/spark-3.5.1-bin-hadoop3"
mongo_uri = "mongodb+srv://shoon9525:WmOMn1vTg65pnXID@cluster0.iaom9xh.mongodb.net/myFirstDatabase"
mongo_db = "NBA2023"
mongo_collection = "newCol"

# Create Spark session with MongoDB configuration
spark = SparkSession \
    .builder \
    .appName("NBA Player Stats Explorer for 2023 with Spark") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.mongodb.input.database", mongo_db) \
    .config("spark.mongodb.input.collection", mongo_collection) \
    .getOrCreate()

# Load data from MongoDB into a Spark DataFrame
playerstats_spark = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Example usage of Spark DataFrame: filtering players by team
# You can replace pandas operations with Spark DataFrame operations
selected_team = ['Lakers', 'Warriors']  # example teams
filtered_data_spark = playerstats_spark.filter(playerstats_spark.TEAM.isin(selected_team))

# Show the filtered data
filtered_data_spark.show()

# Update your Streamlit UI components to handle Spark DataFrames
import streamlit as st

st.title('NBA Player Stats Explorer for 2023 - Spark Version')
if not filtered_data_spark.rdd.isEmpty():
    st.dataframe(filtered_data_spark.toPandas())  # converting Spark DF to pandas DF for Streamlit display
else:
    st.error("No player stats available. Check the MongoDB data.")
