from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("DataAnalysis") \
    .config("spark.hadoop.fs.s3a.access.key", "{access_key}") \
    .config("spark.hadoop.fs.s3a.secret.key", "{secret_key}") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Set log level to WARN
spark.sparkContext.setLogLevel("WARN")
	
df = spark.read.csv("s3a://bda-mini-project-bucket/dataset/games_transformed.csv", header=True, inferSchema=True)

print("Length of dataset: ", df.count())

# Register DataFrame as a temporary view
df.createOrReplaceTempView("games")

# SQL Queries for Data Analysis

# 1. Identify Top-Performing Games by User Recommendations
top_games = spark.sql("""
    SELECT Name, Recommendations
    FROM games
    ORDER BY Recommendations DESC
    LIMIT 10
""")

# 2. Analyze Month-Over-Month Revenue Growth
revenue_growth = spark.sql("""
    SELECT 
        SUBSTR(`Release date`, 1, 7) AS ReleaseMonth,
        AVG(Price) AS AvgPrice,
        AVG(Price) - LAG(AVG(Price), 1) OVER (ORDER BY SUBSTR(`Release date`, 1, 7))
        AS MonthOverMonthGrowth
    FROM games
    WHERE Price > 0
    GROUP BY ReleaseMonth
    ORDER BY ReleaseMonth
""")

# 3. Determine the Most Popular Game Categories
popular_categories = spark.sql("""
    SELECT 
        TRIM(Category) AS Category,
        COUNT(*) AS GameCount
    FROM (
        SELECT EXPLODE(SPLIT(Categories, ',')) AS Category
        FROM games
    )
    GROUP BY Category
    ORDER BY GameCount DESC
    LIMIT 10
""")

# 4. Analyze Game Availability Across Platforms
game_availability = spark.sql("""
    SELECT 
        CASE 
            WHEN Windows = 'true' AND Mac = 'true' AND Linux = 'true' THEN 'Windows, Mac, Linux'
            WHEN Windows = 'true' AND Mac = 'true' THEN 'Windows, Mac'
            WHEN Windows = 'true' AND Linux = 'true' THEN 'Windows, Linux'
            WHEN Mac = 'true' AND Linux = 'true' THEN 'Mac, Linux'
            WHEN Windows = 'true' THEN 'Windows'
            WHEN Mac = 'true' THEN 'Mac'
            WHEN Linux = 'true' THEN 'Linux'
            ELSE 'Other'
        END AS Platforms,
        COUNT(*) AS GameCount
    FROM games
    GROUP BY Platforms
    ORDER BY GameCount DESC
""")

# 5. Identify Games with High User Engagement
high_engagement = spark.sql("""
    SELECT 
        Name,
        (Positive / (Positive + Negative)) * 100 AS EngagementRate
    FROM games
    WHERE Positive + Negative > 0
    ORDER BY EngagementRate DESC
    LIMIT 10
""")

# 6. Impact of DLC count on Recommendations
dlc_impact = spark.sql("""
    SELECT 
        `DLC count`, 
        AVG(Recommendations) AS AvgRecommendations
    FROM games
    GROUP BY `DLC count`
    ORDER BY `DLC count`
    LIMIT 10
""")

# Display results
print("Top Performing Games:")
top_games.show()

print("\nMonth-Over-Month Revenue Growth:")
revenue_growth.show()

print("\nMost Popular Game Categories:")
popular_categories.show()

print("\nGame Availability Across Platforms:")
game_availability.show()

print("\nGames with High User Engagement:")
high_engagement.show()

print("\nImpact of DLC count on Recommendations:")
dlc_impact.show()

# Stop the Spark session
spark.stop()
