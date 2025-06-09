# 🚀 Single-Source Shortest Path with PySpark

This project implements the **Single-Source Shortest Path (SSSP)** algorithm using **Apache Spark (PySpark)**. It operates on a Twitter follower graph and calculates the shortest distance from a starting node to all reachable nodes in the graph.

---

## 📁 Dataset Format

The graph is stored as a CSV with directed edges: follower_id,user_id

Example:
12,13
12,14

In this format, users 13 and 14 follow user 12.

---

## 💡 Algorithm Overview

- The graph is represented as an RDD of edges: `(follower_id, user_id)`
- The SSSP algorithm tracks the shortest distance `d` from a starting node to each reachable node.
- Iteratively, nodes with a known distance `d` propagate `d+1` to their neighbors.
- After several iterations, we count how many nodes are reachable at each distance.

---

## 🧠 Output Format

distance number_of_nodes
0 1
1 3
2 7


Only nodes with finite distance (reachable from the source) are included.

---

## 📦 Files

- `graph.py` – Main PySpark script that implements the algorithm.
- `graph.local.run` – SLURM script to run in **local mode** for small graph testing.
- `graph.distr.run` – SLURM script to run in **distributed mode** on HPC.
- `small-graph.csv` – Sample graph for testing.
- `small-output.txt` – Result file.

---

## 🧪 Run Instructions

### `graph.local.run`
This SLURM script runs the Single-Source Shortest Path algorithm on a small graph file in local mode (1 node).  
Useful for testing correctness before scaling.

```bash
sbatch graph.local.run

### `graph.distr.run`

This SLURM batch script runs the **Single-Source Shortest Path** algorithm on the full Twitter follower graph in **distributed mode**.


**Run with:**
```bash
sbatch graph.distr.run
