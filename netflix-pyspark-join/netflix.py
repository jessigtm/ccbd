import sys

from pyspark import SparkContext
sc = SparkContext()
sc.getConf().setAppName("Netflix")

def parse_ratings(line):
    movieId, userId, rating, date = line.split(",")
    return (movieId, float(rating))

ratings = sc.textFile(sys.argv[1]).map(parse_ratings)

def parse_titles(line):
    lines = line.split(",")
    movieId = lines[0]
    year = lines[1] if lines[1] != "NULL" else "Unknown"  
    title = ",".join(lines[2:])
    return (movieId, (year, title))

titles = sc.textFile(sys.argv[2]).map(parse_titles)

joining = ratings.map(lambda x: (x[0], x[1])) \
    .join(titles) \
    .mapValues(lambda v: (v[0], v[1])) 

avgratings = joining.mapValues(lambda v: v[0]) \
    .groupByKey() \
    .mapValues(lambda ratings: sum(ratings) / len(ratings))

res1 = avgratings.join(titles) \
    .map(lambda x: (x[0], x[1][0], x[1][1][0], x[1][1][1]))  

res2 = res1.sortBy(lambda x: x[1], ascending=False)  

res = res2.map(lambda x: f"{x[1]:.2f}    {x[2]}: {x[3]}")

res.coalesce(1).saveAsTextFile(sys.argv[3])

sc.stop()
