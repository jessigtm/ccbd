# 🎬 Netflix Movie Ratings Join (PySpark)

This project implements a **Reduce-Side Join** using **PySpark (Core API)** to merge **movie ratings** with **movie metadata**.

The input consists of:

- `ratings.txt`: movie ratings in the format `movieID,userID,rating,YYYY-MM-DD`
- `movie_titles.csv`: movie metadata in the format `movieID,year,title`

---

## Objective

Join both datasets to output: avg_rating year: title

for each movie, where:

- `avg_rating`: average rating (float)
- `year`: release year (handle `NULL` values gracefully)
- `title`: complete movie title (reconstructed if it contains commas)

---

## Requirements

- Use **PySpark Core API only**
- Written entirely in **Python**
- Output should:
  - Be sorted by `avg_rating` **in descending order**
  - Handle **commas** in titles
  - Handle **NULL year** values

---

## Run Instructions

### Local Run (example)

```bash
spark-submit netflix.py small-ratings.txt small-titles.txt output
cat output/part* > small-output.txt


Distributed Run (example SLURM call)
sbatch netflix.distr.run
