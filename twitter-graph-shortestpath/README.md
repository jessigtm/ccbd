# Twitter Graph Shortest Path (MapReduce)

This project implements an **iterative MapReduce-based Single-Source Shortest Path (SSSP)** algorithm on a directed Twitter follower graph using **Hadoop**.

It demonstrates how to perform large-scale graph processing using multiple MapReduce stages, running on both local and distributed environments such as HPC clusters (e.g., Expanse).

---

## Problem Description

Given a directed graph of the form: user_id, follower_id


This program calculates the shortest distance from a single source user to all reachable users in the graph using the following stages:

1. Convert the edge list to a compact representation.
2. Iteratively propagate shortest distances using reduce-side joins (5 rounds).
3. Output reachable nodes and their distances from the source.

---

## Dataset

The real-world dataset used is a snapshot of the **Twitter follower graph** from 2010, containing:

- **736,930 users**
- **36,743,448 links**

A smaller test file `small-graph.csv` is also provided for local testing.

---

## Build Instructions

Use the provided build script to compile the MapReduce code:

```bash
./graph.build


## Run Instructions

### `graph.local.run`

This SLURM script runs the Single-Source Shortest Path algorithm on a **small graph file in local mode (1 node)**.  
Useful for testing correctness before scaling.


```bash
sbatch graph.local.run

### `graph.distr.run`

This SLURM batch script runs the **full Twitter follower graph** in **distributed mode** using **MyHadoop** on a multi-node HPC cluster.

```bash
sbatch graph.distr.run
