import ray
import sys
import os
import ray.data
from ray.data import DataContext
DataContext.get_current().execution_options.verbose_progress = True

uniform_hash = lambda k, num: int(k) % num
# a dataset is a list of key-value pairs, num is the number of reducers
# Partition the dataset into num lists of key-value pairs using uniform hashing
@ray.remote
def map_task ( dataset, num ):
    partitions = [[] for _ in range(num)]
    for k, v in dataset:
        i = uniform_hash(k, num)
        partitions[i].append((k, v))
    return partitions

# from the left and right partitions (generated from the map_tasks), return
#    their cogroup (a dictionary from a key to a pair of lists of values)
@ray.remote
def reduce_task ( left, right ):
    results = {}
    for part in left:
        for k, v in part:
            if k not in results:
                results[k] = ([], [])
            results[k][0].append(v)
    for part in right:
        for k, v in part:
            if k not in results:
                results[k] = ([], [])
            results[k][1].append(v)
    return results

# left and right are lists of partitions, num is the number of reducers,
#    each partition is a list of key-value pairs.
# Return a dictionary from a key to a pair of lists of values
def cogroup ( left, right, num ):
    # a list of map_task from the left
    left_map_tasks = [map_task.remote(partition, num) for partition in left]
    # a list of map_task from the right
    right_map_tasks = [map_task.remote(partition, num) for partition in right]

    left_map_results = ray.get(left_map_tasks)
    right_map_results = ray.get(right_map_tasks)
    # a list of reduce_tasks where the ith task gets the input
    #    from the ith outputs of left_map_tasks and right_map_tasks
    reduce_tasks =  []
    for i in range(num):
        lp = [result[i] for result in left_map_results]
        rp = [result[i] for result in right_map_results]
        reduce_tasks.append(reduce_task.remote(lp, rp))
    return {k: v for s in ray.get(reduce_tasks)
              for k, v in s.items()}

def parse_ratings_file(filename):
    ds = ray.data.read_text(filename, encoding="latin-1")
    result = []
    size = 200000

    for batch in ds.iter_batches(batch_size=size, batch_format="pandas"):
        part = []
        for line in batch["text"]:
            fields = line.strip().split(",")
            part.append((fields[0], float(fields[2])))
        result.append(part)

    return result

def parse_movies_file(filename):
    ds = ray.data.read_text(filename, encoding="latin-1")
    result = []
    size = 10000

    for batch in ds.iter_batches(batch_size=size, batch_format="pandas"):
        part = []
        for line in batch["text"]:
            fields = line.strip().split(",", 2)
            year = "Unknown" if fields[1] == "NULL" else fields[1]
            part.append((fields[0], (year, fields[2])))
        result.append(part)

    return result

if __name__ == "__main__":
    ray.init(address=os.environ["ip_head"])
    # read the ratings from the file: a list of partitions
    ratings = parse_ratings_file(sys.argv[1])
    print("Number of batches in the ratings dataset:",len(ratings), flush=True)
    # read the movie titles from the file: a list of partitions
    movies = parse_movies_file(sys.argv[2])
    print("Number of batches in the movies dataset:",len(movies), flush=True)
    for k,v in cogroup(ratings,movies,10).items():
        if len(v[0]) > 0:
           avg_rating = sum(v[0])/len(v[0])
           title = v[1][0][1]
           print(f"{avg_rating:.3f}\t{title}", flush=True)
    ray.shutdown()