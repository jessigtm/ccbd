import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, asc, desc

spark = SparkSession.builder.appName('Netflix') \
            .getOrCreate()

sc = spark.sparkContext


# Ratings
ratings = sc.textFile(sys.argv[1])
dfratings = ratings.map(lambda line: line.split(",")).map(lambda parts: (parts[0], float(parts[2]))).toDF(["movieId", "rating"])

# Titles
titles = sc.textFile(sys.argv[2])
dftitles = titles.map(lambda line: line.split(",")) \
    .map(lambda parts: (
        parts[0],
        parts[1] if parts[1] != "NULL" else "Unknown",
        ",".join(parts[2:])
    )).toDF(["movieId", "year", "title"])

# Getting the average
avg_ratings_df = dfratings.groupBy("movieId").agg(avg("rating").alias("avg"))

# Join
df = avg_ratings_df.join(dftitles, on="movieId").select("avg", "year", "title").orderBy(desc("avg"))

print("DataFrame results:")
for x in df.collect():
   print(f"{x.avg:1.3f}\t{x.year}   {x.title}")


dfratings.createOrReplaceTempView("ratings")
dftitles.createOrReplaceTempView("titles")

query = spark.sql("""
    SELECT AVG(r.rating) AS avg, t.year, t.title
    FROM ratings r
    JOIN titles t ON r.movieId = t.movieId
    GROUP BY t.movieId, t.year, t.title
    ORDER BY avg DESC
""")

print("SQL query results:")
for x in query.collect():
   print(f"{x.avg:1.3f}\t{x.year}   {x.title}")
