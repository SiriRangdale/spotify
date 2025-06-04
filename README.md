##Spotify Genre-Based Insights using Medallion Architecture in Azure Databricks and Power BI

Objective:
To analyze a large Spotify dataset by applying the Medallion Architecture in Azure Databricks, standardize inconsistent genre values, and derive actionable insights using a Power BI dashboard.

Tools & Technologies:
Azure Databricks (PySpark & SQL) – for data transformation

Medallion Architecture (Bronze, Silver, Gold) – for scalable data pipeline

Power BI – for data visualization

GitHub – for version control and collaboration

Dataset:
Source: Spotify dataset from Kaggle

Contains: Track metadata, artist info, popularity, energy, valence, tempo, and genre

Key Steps:
1. Bronze Layer – Raw Data Ingestion
Loaded raw .csv file into Databricks.

Performed basic schema inference and initial validation.

2. Silver Layer – Data Cleaning & Enrichment
Cleaned null values, corrected data types.

Standardized the genres column which had fragmented values (e.g., "dance pop", "alt z pop", "electropop") by grouping them into broad genres like Pop, Electronic, Hip-Hop, etc.

3. Gold Layer – Business-Ready Data
Created a new column main_genre using SQL CASE WHEN logic to classify all sub-genres into ~10 main genres.
Added Other Use Cases such as Top 3 artsists based on Popularity and Followers as well.

Aggregated artist and track-level insights by genre.

4. Power BI Dashboard
Imported Gold data.
Created interactive visuals:
Top genres by unique artist count
Top 3 artsists based on Popularity
Top 3 artsists based on Followers
Filters by year, genre, or artist

📈 Insights Discovered:
Pop genre had the highest number of unique artists.
Tailor Swift , Arctic Monkey and Ariana Grande were found to be the top 3 artsists based on Popularity.
Arjit Singh, Tailor Swift and Ed sheeran were found to be the Top 3 artsists based on followers.
