# Netflix Ratings Analysis with MapReduce

This project demonstrates two MapReduce jobs written in Java using Hadoop:
1. **Count total reviews per movie.**
2. **Compute average movie rating per year.**

The program is designed to run on distributed systems like Expanse (SDSC).

## Build Instructions
```bash
./netflix.build

### `netflix.local.run`
This SLURM script runs the Netflix MapReduce job on a single node in local mode (non-distributed) for testing purposes. Paths and module versions should be updated according to the cluster environment.

### `netflix.distr.run` 
This SLURM batch script is designed to run the Netflix MapReduce job in distributed mode across multiple nodes using MyHadoop on an HPC environment. Paths (`HADOOP_HOME`, `MYHADOOP_HOME`, etc.) should be configured based on system configuration.