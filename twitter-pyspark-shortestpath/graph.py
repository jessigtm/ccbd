import sys

from pyspark import SparkContext, SparkConf
sc = SparkContext()
sc.getConf().setAppName("Graph")

start_id =  1#14701391 
max_int = sys.maxsize
iterations = 5

# from the input file, create an RDD of graph edges (i,j), where i follows j
graph = sc.textFile(sys.argv[1]) \
          .map(lambda line: line.strip().split(",")) \
          .map(lambda key: (int(key[1]), int(key[0]))) \
          .groupByKey() \
          .mapValues(list)

# calculate the initial shortest distances R, which is an RDD of (id,distance)
#    (the initial distance of the starting point is 0, and of the others is max_int)
n = graph.keys().distinct() \
   .union(graph.flatMap(lambda x: x[1]).distinct())

# Assign initial distances
R = n.map(lambda x: (x, 0 if x == start_id else max_int))

for i in range(iterations):
   # find the new shortest distances R from the current R and graph
    n_join = R.join(graph)  
    dist = n_join.flatMap(lambda x: [(j, x[1][0] + 1) for j in x[1][1]])
    R = R.union(dist) \
      .reduceByKey(min)

# for each different distance, count the number of nodes that have this distance
#   (use only the vertices that can be reached: distance < max_int)
results = R.filter(lambda x: x[1] < max_int) \
.map(lambda x: (x[1], 1)) \
.reduceByKey(lambda a, b: a + b)     

for v in results.collect():
   print(v)
