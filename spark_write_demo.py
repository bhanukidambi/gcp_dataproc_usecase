# create sparksession #
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import *

############################################## Usecase code ##############################################
gcs_input_path = "gs://usecase-swift-hope-391116/data/generated_data.csv"
gcs_output_path = "gs://usecase-swift-hope-391116/data/output/"

# Can also do this to read #
#df_automobile = spark.read.csv("gs://usecase-swift-hope-391116/data/generated_data.csv", header=True)

print("Reading Input file")
df_automobile = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(gcs_input_path)


# # Creating temporary view to test the query #
# #print("Creating temp view")
# df_automobile.createOrReplaceTempView("automobile")

# # Performs basic analysis of dataset
# df_auto = spark.sql("""
# SELECT * FROM automobile
# """).cache()

# Removing Duplicates #
df_final = df_automobile.dropDuplicates()

# Saves results to single CSV file in Google Storage Bucket
print("Overwriting the output")

df_final.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(gcs_output_path)

## To save the results in a hive table ##
# df_final.write.saveAsTable("automobile_output")

# Stop the spark application after the job #
#spark.stop()


