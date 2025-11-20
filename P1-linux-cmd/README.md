# Linux CMD project

This project limited on tool we are allowed to use, no pyhton, no SQL. We are encouraged to use awk() command :)

### Data
https://raw.githubusercontent.com/yinghaoz1/tmdb-movie-dataset-analysis/master/tmdb-movies.csv

**Context:** The data is stored on a Linux server. The Data Engineering team needs to use Linux command-line tools to support the following tasks in order to obtain basic information about the dataset
The tasks came from the following brief:

	1.	Sort the movies by release date in descending order and save the result to a new file.
	2.	Filter the movies with an average rating above 7.5 and save them to a new file.
	3.	Identify the movie with the highest revenue and the one with the lowest revenue.
	4.	Calculate the total revenue of all movies.
	5.	List the top 10 movies with the highest profit.
	6.	Determine which director has the most movies and which actor appears in the most movies.
	7.	Generate statistics on the number of movies by genre. For example, how many movies fall under Action, Family, etc.
	8.	Your own ideas for additional analysis on this dataset?

### How to run

1. Install [`csvkit`](https://csvkit.readthedocs.io/) so `csvsql` and `csvcut` are on your `$PATH`.
2. Execute the automation script from this directory:
   ```bash
   bash movies_analysis.sh
   ```
   The script downloads `tmdb-movies.csv` if it is missing, then runs every analysis using pure command-line utilities.

### Produced artifacts

- `movies_sorted.csv` – dataset ordered by release date (newest first, with month/day sorting handled in SQLite).
- `movies_rating_gt_7_5.csv` – only movies where `vote_average > 7.5`, sorted by rating and vote count.
- `revenue_highest_and_lowest.csv` – two-row file showing the most and least lucrative releases.
- `revenue_total.csv` – single-row aggregate of total revenue across all rows.
- `top10_profit.csv` – profit (`revenue - budget`) leaderboard.
- `director_with_most_movies.csv` and `actor_with_most_movies.csv` – most prolific contributors by role.
- `genre_stats.csv` – counts of films per genre tag.
- `revenue_and_rating_by_year.csv` – bonus analysis summarizing yearly totals, mean rating, and movie volume.


