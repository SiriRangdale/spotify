# Databricks notebook source
service_credential = dbutils.secrets.get(scope="akv_db_scope_training",key="dbtrainingspnsecret")

spark.conf.set("fs.azure.account.auth.type.azstoragetraining.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azstoragetraining.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azstoragetraining.dfs.core.windows.net", "3f91e76c-33f1-4ec5-bb48-a65ceebe2b98")
spark.conf.set("fs.azure.account.oauth2.client.secret.azstoragetraining.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azstoragetraining.dfs.core.windows.net", "https://login.microsoftonline.com/45da25de-8003-4abf-bea8-55aba615e5e7/oauth2/token")

# COMMAND ----------

spotify = spark.read.format('csv') \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("mergeSchema", True)\
    .load('abfss://spotify@azstoragetraining.dfs.core.windows.net')

# COMMAND ----------

spotify.display()

# COMMAND ----------

spotify = spotify.dropna()


# COMMAND ----------

spotify.display()

# COMMAND ----------

spotify = spotify.drop_duplicates()

# COMMAND ----------

print("Total rows after cleaning:", spotify.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Drop unwanted column

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date, year
spotify = spotify.drop("Unnamed: 0")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Handle missing genres

# COMMAND ----------

spotify = spotify.withColumn("genres", when(col("genres").isNull(), "Unknown").otherwise(col("genres")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Convert release_date to proper date format and extract release_year

# COMMAND ----------

spotify = spotify.withColumn("release_date", to_date(col("release_date"), "dd-MM-yyyy"))
spotify = spotify.withColumn("release_year", year(col("release_date")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Create duration in minutes

# COMMAND ----------

spotify = spotify.withColumn("duration_min", col("duration_ms") / 60000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove duplicates

# COMMAND ----------

spotify = spotify.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Remove records with any nulls

# COMMAND ----------

spotify = spotify.na.drop()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Show final row count

# COMMAND ----------

print("Total rows after cleaning:", spotify.count())


# COMMAND ----------

spotify = spotify.drop("_c0")


# COMMAND ----------

spotify.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a temporary view from the DataFrame

# COMMAND ----------


spotify.createOrReplaceTempView("spotify_view")


# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Top 3 Artists by Popularity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artist_name, MAX(artist_popularity) AS popularity
# MAGIC FROM spotify_view
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY popularity DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Top 3 Artists by Followers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artist_name, MAX(followers) AS max_followers
# MAGIC FROM spotify_view
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY max_followers DESC
# MAGIC LIMIT 3;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Count of Artists by Genre

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN LOWER(genres) LIKE '%dance pop%' THEN 'Pop'
# MAGIC     WHEN LOWER(genres) LIKE '%alt z%' THEN 'Pop'
# MAGIC     WHEN LOWER(genres) LIKE '%pop%' THEN 'Pop'
# MAGIC     WHEN LOWER(genres) LIKE '%edm%' THEN 'Electronic'
# MAGIC     WHEN LOWER(genres) LIKE '%electropop%' THEN 'Electronic'
# MAGIC     WHEN LOWER(genres) LIKE '%etherpop%' THEN 'Electronic'
# MAGIC     WHEN LOWER(genres) LIKE '%electronica%' THEN 'Electronic'
# MAGIC     WHEN LOWER(genres) LIKE '%ambient%' THEN 'Electronic'
# MAGIC     WHEN LOWER(genres) LIKE '%hip hop%' THEN 'Hip-Hop'
# MAGIC     WHEN LOWER(genres) LIKE '%rap%' THEN 'Hip-Hop'
# MAGIC     WHEN LOWER(genres) LIKE '%trap%' THEN 'Hip-Hop'
# MAGIC     WHEN LOWER(genres) LIKE '%melodic rap%' THEN 'Hip-Hop'
# MAGIC     WHEN LOWER(genres) LIKE '%neo soul%' THEN 'R&B'
# MAGIC     WHEN LOWER(genres) LIKE '%r&b%' THEN 'R&B'
# MAGIC     WHEN LOWER(genres) LIKE '%soul%' THEN 'R&B'
# MAGIC     WHEN LOWER(genres) LIKE '%k-pop%' THEN 'K-Pop'
# MAGIC     WHEN LOWER(genres) LIKE '%rock%' THEN 'Rock'
# MAGIC     WHEN LOWER(genres) LIKE '%punk%' THEN 'Rock'
# MAGIC     WHEN LOWER(genres) LIKE '%metal%' THEN 'Metal'
# MAGIC     WHEN LOWER(genres) LIKE '%reggaeton%' THEN 'Latin'
# MAGIC     WHEN LOWER(genres) LIKE '%latin pop%' THEN 'Latin'
# MAGIC     WHEN LOWER(genres) LIKE '%urbano latino%' THEN 'Latin'
# MAGIC     WHEN LOWER(genres) LIKE '%bachata%' THEN 'Latin'
# MAGIC     WHEN LOWER(genres) LIKE '%latin hip hop%' THEN 'Latin'
# MAGIC     WHEN LOWER(genres) LIKE '%classical%' THEN 'Classical'
# MAGIC     WHEN LOWER(genres) LIKE '%baroque%' THEN 'Classical'
# MAGIC     WHEN LOWER(genres) LIKE '%country%' THEN 'Country'
# MAGIC     WHEN LOWER(genres) LIKE '%singer-songwriter%' THEN 'Singer-Songwriter'
# MAGIC     WHEN LOWER(genres) LIKE '%indie%' THEN 'Indie'
# MAGIC     WHEN LOWER(genres) LIKE '%afrobeats%' THEN 'Afrobeats'
# MAGIC     ELSE 'Other'
# MAGIC   END AS main_genre,
# MAGIC   COUNT(DISTINCT artist_name) AS unique_artist_count
# MAGIC FROM spotify_view
# MAGIC GROUP BY main_genre
# MAGIC ORDER BY unique_artist_count DESC;
# MAGIC

# COMMAND ----------

spotify.display()

# COMMAND ----------

# Create a permanent table for Power BI to read
spotify.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("spotify_table")

