#!/bin/bash
# Analyze TMDB movie data using csvkit-friendly command-line tooling.
# Each block below echoes the goal then produces the requested artifact.

set -euo pipefail

DATA_URL="https://raw.githubusercontent.com/yinghaoz1/tmdb-movie-dataset-analysis/master/tmdb-movies.csv"
RAW_FILE="tmdb-movies.csv"
TABLE_NAME="movies"

# Download the dataset once so the rest of the commands have a local source file.
if [[ ! -f "$RAW_FILE" ]]; then
  echo "[INFO] Downloading dataset..."
  curl -L -o "$RAW_FILE" "$DATA_URL"
else
  echo "[INFO] Dataset already present at $RAW_FILE"
fi

# Task 1: Sort movies by release date (descending) using sqlite-backed csvsql logic.
echo "[INFO] Sorting movies by release date..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
WITH enhanced AS (
  SELECT *,
         CASE
           WHEN instr(release_date,'/') > 0
           THEN CAST(substr(release_date, 1, instr(release_date,'/') - 1) AS INTEGER)
           ELSE NULL
         END AS release_month,
         CASE
           WHEN instr(release_date,'/') > 0
            AND instr(substr(release_date, instr(release_date,'/') + 1), '/') > 0
           THEN CAST(substr(
                      substr(release_date, instr(release_date,'/') + 1),
                      1,
                      instr(substr(release_date, instr(release_date,'/') + 1), '/') - 1
                    ) AS INTEGER)
           ELSE NULL
         END AS release_day
  FROM movies
)
SELECT * FROM enhanced
ORDER BY CAST(release_year AS INTEGER) DESC,
         release_month DESC,
         release_day DESC
SQL
)" "$RAW_FILE" > movies_sorted.csv

# Task 2: Filter movies whose vote_average is greater than 7.5 and keep highest-rated first.
echo "[INFO] Filtering movies with vote_average > 7.5..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
SELECT *
FROM movies
WHERE CAST(vote_average AS FLOAT) > 7.5
ORDER BY CAST(vote_average AS FLOAT) DESC, vote_count DESC
SQL
)" "$RAW_FILE" > movies_rating_gt_7_5.csv

# Task 3: Capture both the highest and lowest revenue entries in a single CSV file.
echo "[INFO] Extracting highest and lowest revenue movies..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
SELECT 'highest' AS revenue_rank, *
FROM (
  SELECT *
  FROM movies
  ORDER BY CAST(revenue AS INTEGER) DESC
  LIMIT 1
)
UNION ALL
SELECT 'lowest' AS revenue_rank, *
FROM (
  SELECT *
  FROM movies
  ORDER BY CAST(revenue AS INTEGER) ASC
  LIMIT 1
)
SQL
)" "$RAW_FILE" > revenue_highest_and_lowest.csv

# Task 4: Sum the revenue column to get the global total revenue figure.
echo "[INFO] Computing total revenue..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
SELECT SUM(CAST(revenue AS FLOAT)) AS total_revenue
FROM movies
SQL
)" "$RAW_FILE" > revenue_total.csv

# Task 5: Calculate profit per movie (revenue - budget) and list the largest ten.
echo "[INFO] Listing top 10 most profitable movies..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
SELECT original_title,
       CAST(revenue AS FLOAT) - CAST(budget AS FLOAT) AS profit,
       revenue,
       budget
FROM movies
ORDER BY profit DESC
LIMIT 10
SQL
)" "$RAW_FILE" > top10_profit.csv

# Task 6a: Determine which director has the most credited movies.
echo "[INFO] Finding most prolific director..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1
SQL
)" "$RAW_FILE" > director_with_most_movies.csv

# Task 6b: Determine which actor appears in the most movies by expanding the pipe-delimited cast column.
echo "[INFO] Finding actor with the most movie appearances..."
actor_tmp=$(mktemp)
csvcut -c cast "$RAW_FILE" \
  | tail -n +2 \
  | tr '|' '\n' \
  | sed '/^\s*$/d' \
  | sort \
  | uniq -c \
  | sort -nr > "$actor_tmp"
{
  echo "actor,movie_count"
  awk 'NR==1 {count=$1; $1=""; sub(/^ +/,""); printf "\"%s\",%s\n",$0,count}' "$actor_tmp"
} > actor_with_most_movies.csv
rm -f "$actor_tmp"

# Task 7: Count the number of movies associated with each genre.
echo "[INFO] Generating genre distribution..."
{
  echo "genre,movie_count"
  csvcut -c genres "$RAW_FILE" \
    | tail -n +2 \
    | tr '|' '\n' \
    | sed '/^\s*$/d' \
    | sort \
    | uniq -c \
    | sort -nr \
    | awk '{count=$1; $1=""; sub(/^ +/,""); printf "\"%s\",%s\n",$0,count}'
} > genre_stats.csv

# Task 8: Extra analysis idea – yearly revenue and average rating trends for quick macro insights.
echo "[INFO] Calculating yearly revenue and rating trends..."
csvsql --tables "$TABLE_NAME" --query "$(cat <<'SQL'
SELECT release_year,
       SUM(CAST(revenue AS FLOAT)) AS total_revenue,
       AVG(CAST(vote_average AS FLOAT)) AS avg_rating,
       COUNT(*) AS movie_count
FROM movies
WHERE release_year IS NOT NULL AND release_year != ''
GROUP BY release_year
ORDER BY release_year
SQL
)" "$RAW_FILE" > revenue_and_rating_by_year.csv

echo "[DONE] Analytics artifacts generated."
