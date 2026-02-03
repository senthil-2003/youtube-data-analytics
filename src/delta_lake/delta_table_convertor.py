from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("SimplePySparkApp") \
    .getOrCreate()

# Create sample data
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name"])

# Show DataFrame
df.show()

# Stop Spark session
spark.stop()