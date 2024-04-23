# install necessary libraries for spark
# create spark session


import os
import sys
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/Users/hoon/Downloads/spark-3.5.1-bin-hadoop3"


import findspark
findspark.init()
findspark.find()

import pyspark

from pyspark.sql import DataFrame, SparkSession
from typing import List
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark= SparkSession \
       .builder \
       .appName("DSCI 351 Spark") \
       .getOrCreate()

# Path to the CSV file
csv_file_path = "/Users/hoon/Desktop/streamLit/newNba.csv"

# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_file_path)

# Show the first few rows of the DataFrame to verify it's loaded correctly
df.show()
