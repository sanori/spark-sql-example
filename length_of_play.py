from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import os.path

# Start Spark
spark = SparkSession \
    .builder \
    .appName("pyspark example") \
    .getOrCreate()

basedir = os.path.dirname(os.path.realpath(__file__))

# Read all of Shakespeare's plays
df = spark.read.parquet(os.path.join(basedir,
    "data/shakespeare.gz.parquet"))

# Print the schema to the console
df.printSchema()

# Calculate the number of lines for each work (play)
result = df \
    .groupBy("play_name") \
    .agg(sf.count("line_id").alias("lines")) \
    .orderBy("lines", ascending=False)

# Print a part of the result to the console
result.show()

# Save the result as one file in JSON Lines format
result \
    .repartition(1) \
    .write \
    .json(os.path.join(basedir, "length_of_play"), mode="overwrite")

# Stop Spark
spark.stop()
