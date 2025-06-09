# 🎬 Netflix Data Analysis with Spark DataFrames & SQL

This project performs a join between Netflix movie ratings and movie titles using **PySpark**, implemented in **two ways**:

1. Using **Spark DataFrame** methods  
2. Using **Spark SQL**

---

## Objective

- Join two datasets:
  - `ratings.txt`: contains user ratings
  - `movie_titles.csv`: contains movie metadata
- Compute the **average rating** for each movie
- Handle:
  - Titles containing commas (combine all parts)
  - Missing years (treat `NULL` as `0`)
- Sort results by **average rating (descending)**
- Return the **Top 100** highest-rated movies

---

##  Run Instructions

### `netflix.local.run`

Runs the Spark job on a single node using local mode. Useful for testing with small data.

```bash
sbatch netflix.local.run

### `netflix.distr.run`

This SLURM batch script runs the Spark job on a distributed HPC cluster using MyHadoop and Spark.


#### Run the script:

```bash
sbatch netflix.distr.run
