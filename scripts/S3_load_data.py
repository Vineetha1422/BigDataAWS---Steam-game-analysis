from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadData") \
    .config("spark.hadoop.fs.s3a.access.key", "{access_key}") \
    .config("spark.hadoop.fs.s3a.secret.key", "{secret_key}") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()
	
data = spark.read.csv("s3a://bda-mini-project-bucket/dataset/games.csv", header=True, inferSchema=True)

data.printSchema()
data.show(5)

spark.stop()