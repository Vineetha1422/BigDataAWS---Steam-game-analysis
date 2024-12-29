from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, split, desc
from pyspark.sql.functions import mean, stddev, min, max, avg
from pyspark.sql.functions import datediff, current_date, to_date
from pyspark.sql.functions import year, month, count
from pyspark.sql.functions import array_contains

spark = SparkSession.builder \
    .appName("DataTransformation") \
    .config("spark.hadoop.fs.s3a.access.key", "{access_key}") \
    .config("spark.hadoop.fs.s3a.secret.key", "{secret_key}") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()
    
# Set log level to WARN
spark.sparkContext.setLogLevel("WARN")
	
df = spark.read.csv("s3a://bda-mini-project-bucket/dataset/games.csv", header=True, inferSchema=True)

#Null values handling
print("=================Null Value Handling==================")
null_counts = df.select([(col(c).isNull().cast("int")).alias(c) for c in df.columns]).groupBy().sum()

for column, c in zip(null_counts.columns,null_counts.collect()[0]):
    print(f"{column}: {c} null values")

# Drop rows with null values in specific columns
columns_to_drop_nulls = ['Name', 'Full audio languages', 'Header image', 'Metacritic score', 'Negative', 'Recommendations']
for column in columns_to_drop_nulls:
    df = df.filter(col(column).isNotNull())

# Drop columns
columns_to_drop = ['About the game', 'Reviews', 'Website','Support url', 'Support email', 'Metacritic url', 'Score rank', 'Tags', 'Screenshots','Notes']
df = df.drop(*columns_to_drop)

# Impute null values in 'movies' column
df = df.withColumn('Movies', when(col('Movies').isNull(), 'No movies').otherwise(col('Movies')))

# Handle null values in 'developers', 'publishers', 'categories', 'genres'
columns_to_impute = ['Developers', 'Publishers', 'Categories', 'Genres']
for column in columns_to_impute:
    df = df.withColumn(column, when(col(column).isNull(), 'Unknown').otherwise(col(column)))    

# Print updated null counts
updated_null_counts = df.select([(col(c).isNull().cast("int")).alias(c) for c in df.columns]) \
    .groupBy().sum()

print("-----------------Result------------------")
for column, c in zip(updated_null_counts.columns, updated_null_counts.collect()[0]):
    print(f"{column}: {c} null values")
    


#Checking Outliers
print("===================Outliers check for target variable - Price===========================")
# Calculate basic statistics
price_stats = df.select(
    mean(col("Price")).alias("mean"),
    stddev(col("Price")).alias("stddev"),
    min(col("Price")).alias("min"),
    max(col("Price")).alias("max")
).collect()[0]

mean_price = price_stats["mean"]
stddev_price = price_stats["stddev"]
min_price = price_stats["min"]
max_price = price_stats["max"]

print(f"Price Statistics:\nMean: {mean_price}\nStd Dev: {stddev_price}\nMin: {min_price}\nMax: {max_price}")

# Check top 10 games by price
top_10_games = df.orderBy(col("Price").desc()).limit(10).select("Name", "Price")
print("\n1. Top 10 Most Expensive Games:")
top_10_games.show(truncate=False)

# Drop the row with the highest price
max_price_row = df.orderBy(col("Price").desc()).first()
df = df.filter(col("Price") != max_price_row["Price"])

# Recalculate statistics after dropping the highest price
updated_price_stats = df.select(
    mean(col("Price")).alias("mean"),
    stddev(col("Price")).alias("stddev"),
    min(col("Price")).alias("min"),
    max(col("Price")).alias("max")
).collect()[0]

updated_mean_price = updated_price_stats["mean"]
updated_stddev_price = updated_price_stats["stddev"]
updated_min_price = updated_price_stats["min"]
updated_max_price = updated_price_stats["max"]

print("\nUpdated Price Statistics after dropping the highest price:")
print(f"Mean: {updated_mean_price}\nStd Dev: {updated_stddev_price}\nMin: {updated_min_price}\nMax: {updated_max_price}")

#Duplicates
print("=========================Checking for Duplicate records============================")
duplicate_rows = df.groupBy(df.columns).count().filter("count > 1")
num_duplicate_rows = duplicate_rows.count()
print(f"\nNumber of duplicate rows (all columns): {num_duplicate_rows}")

if num_duplicate_rows > 0:
    print("\nDuplicate rows:")
    duplicate_rows.show()

# Check for duplicate game names
duplicate_count = df.groupBy("Name").count().filter("count > 1").count()
print(f"\nNumber of duplicate game names: {duplicate_count}")

#Transformations
print("====================Transformations=======================")
# Add Game Age column
df = df.withColumn("Release date", to_date(col("Release date"), "MMM d, yyyy"))
df = df.withColumn("Game age", datediff(current_date(), col("Release date")) / 365)

# Add Price Category column
df = df.withColumn("Price category", 
    when(col("Price") < 10, "Low")
    .when((col("Price") >= 10) & (col("Price") <= 50), "Medium")
    .otherwise("High")
)

# Add Popularity Tier column
# Create positive to negative ratio
df = df.withColumn("PN ratio", 
    when(col("Negative") == 0, col("Positive"))  # Handle division by zero
    .otherwise(col("Positive") / col("Negative"))
)
df = df.withColumn("Popularity tier",
    when(
        (col("PN ratio") >= 10) & (col("Achievements") > 100), "Elite"
    ).when(
        (col("PN ratio") >= 5) & (col("Achievements") > 50), "Popular"
    ).when(
        (col("PN ratio") >= 2) & (col("Achievements") > 20), "Rising"
    ).when(
        (col("PN ratio") >= 1), "Average"
    ).otherwise("Niche")
)


# Display transformed columns
df.select("Name","Release date", "Game age", "Price", "Price category","Popularity tier", "Genres").show(5)


# #Aggregations
print("===========================Aggregations==============================")
#1. Average Price by Popularity Tier
avg_revenue_by_popularity = df.groupBy("Popularity tier").agg(avg("Price").alias("avg_price"))
print("======1. Average Price by Popularity Tier======") 
avg_revenue_by_popularity.show()

#2. Distribution of Games Across Popularity Tiers
games_distribution = df.groupBy("Popularity tier").agg(count("Name").alias("num_games"))
print("======2. Distribution of Games Across Popularity Tiers======")
games_distribution.show()

#3. Number of Games Released Per Month
temp_df = df.withColumn("release_month", month("Release date"))
games_per_month = temp_df.groupBy("release_month") \
                                .agg(count("Name").alias("games_per_month"))
games_per_month = games_per_month.orderBy("release_month")
print("======3. Number of Games Released Per Month======") 
games_per_month.show()

#4. Average Achievements by Game Age
avg_achievements_by_age = df.groupBy("Game age") \
                                        .agg(avg("Achievements").alias("avg_achievements")) \
                                        .orderBy(desc("avg_achievements")) \
                                        .limit(10)
print("======4. Average Achievements by Game Age======") 
avg_achievements_by_age.show()

#5. Impact of Free-to-Play Models
# Create a column to distinguish free-to-play games
temp_df = df.withColumn(
    "is_free", when(df["Price"] == 0, "Free").otherwise("Paid")
)
engagement_by_price_model = temp_df.groupBy("is_free") \
                                          .agg(avg("Positive").alias("avg_positive_ratings"))
print("======5. Impact of Free-to-Play Models======")
engagement_by_price_model.show()

#6. Average prices per Genre
#Converting Genres, Categories column from String to list
temp_df = df.withColumn("Genres", split("Genres", ","))
unique_genres = ['360 Video', 'Accounting', 'Action', 'Adventure', 'Animation & Modeling', 'Audio Production', 'Casual', 'Design & Illustration', 'Documentary', 'Early Access',
 'Education', 'Episodic', 'Free to Play', 'Game Development', 'Gore', 'Indie', 'Massively Multiplayer', 'Movie', 'Nudity', 'Photo Editing', 'RPG', 'Racing', 'Sexual Content',
 'Short', 'Simulation', 'Software Training', 'Sports', 'Strategy', 'Tutorial', 'Unknown', 'Utilities', 'Video Production', 'Violent', 'Web Publishing']
genre_avg_prices = []
for genre in unique_genres:
    avg_price = temp_df.filter(array_contains("Genres", genre)) \
                              .agg(avg("price").alias(f"avg_price_{genre}")) \
                              .collect()[0][0]
    genre_avg_prices.append((genre, avg_price))

# Convert to a DataFrame for easier visualization
genre_avg_prices_df = spark.createDataFrame(genre_avg_prices, ["genre", "avg_price"])
print("======6. Average prices per Genre======")
genre_avg_prices_df.orderBy(col("avg_price").desc()).limit(10).show()

#Task 3 : Store the transformed file back to S3 bucket
output_path_csv = "s3a://bda-mini-project-bucket/dataset/games_transformed.csv"
print(df.show(5))
df.write.option("header", "true").csv(output_path_csv)
print("==========Stored file to S3 bucket successfully!==========")

spark.stop()