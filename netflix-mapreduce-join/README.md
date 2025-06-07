# Netflix Ratings & Titles Join with MapReduce

This project performs a **Reduce-Side Join** on two large-scale datasets from Netflix:

1. **Ratings Dataset**  
   Format: `movieID,userID,rating,YYYY-MM-DD`  
   Example: `2048,100,4,2005-08-15`

2. **Movie Titles Dataset**  
   Format: `movieID,year,title`  
   Example: `2048,1999,The Matrix`

---

## Objective

Using MapReduce (in Java), this job joins movie ratings with their corresponding movie titles and release years, producing: year: title avg_rating for each movie, where:
- `year` = release year of the movie
- `title` = movie title
- `avg_rating` = average user rating (float)

This is implemented using the **Reduce-Side Join pattern**, commonly used when datasets must be combined by a common key (`movieID`).

---

## Highlights

- Built using **Java + Hadoop MapReduce**
- Handles large Netflix datasets
- Parses and joins rating + metadata from two files
- Robust error handling for malformed input
- Example technologies demonstrated:
  - Key-based grouping
  - Custom data types (`RatingTitle`)
  - Text format I/O only (no binary)

---
## Run Instructions

### `netflix.build`
This script compiles the Java MapReduce program and packages it into `netflix.jar`. Make sure required modules (Java, Hadoop, etc.) are available on your system.

```bash
./netflix.build

---

### `netflix.local.run`

This SLURM script runs the Netflix MapReduce job on a single node in local mode (non-distributed) for testing purposes.  
Paths and module versions should be updated according to the cluster environment.

---

### `netflix.distr.run` 

This SLURM batch script is designed to run the Netflix MapReduce job in distributed mode across multiple nodes using MyHadoop on an HPC environment.  
Paths (`HADOOP_HOME`, `MYHADOOP_HOME`, etc.) should be configured based on system configuration.
