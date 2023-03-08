from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

# Read CSV file
df = spark.read.csv("s3://my-bucket/my-file.csv", header=True)

# Transform data
df2 = df.select("column1", "column2", "column3")
df3 = df2.withColumn("new_column", df2.column1 + df2.column2)

# Write data to Parquet file
df3.write.mode("overwrite").parquet("s3://my-bucket/my-output.parquet")

# Stop Spark session
spark.stop()
