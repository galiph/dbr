-- Databricks notebook source
-- Which artists released the most songs each year?
SELECT
  artist_name,
  count(artist_name)
AS
  num_songs,
  year
FROM
  databricks_ws_35ef6e3c_9b13_4349_a245_c6e3e49c3f11.default.songs
WHERE
  year > 0
GROUP BY
  artist_name,
  year
ORDER BY
  num_songs DESC,
  year DESC


-- COMMAND ----------

 -- Find songs for your DJ list
 SELECT
   artist_name,
   title,
   tempo
 FROM
  databricks_ws_35ef6e3c_9b13_4349_a245_c6e3e49c3f11.default.songs
 WHERE
   time_signature = 4
   AND
   tempo between 100 and 140;


-- COMMAND ----------


