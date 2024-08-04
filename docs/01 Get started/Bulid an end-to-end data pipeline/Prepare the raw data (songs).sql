-- Databricks notebook source
CREATE OR REPLACE TABLE
  databricks_ws_35ef6e3c_9b13_4349_a245_c6e3e49c3f11.default.songs (
    artist_id STRING,
    artist_name STRING,
    duration DOUBLE,
    release STRING,
    tempo DOUBLE,
    time_signature DOUBLE,
    title STRING,
    year DOUBLE,
    processed_time TIMESTAMP
  );

INSERT INTO
  databricks_ws_35ef6e3c_9b13_4349_a245_c6e3e49c3f11.default.songs
SELECT
  artist_id,
  artist_name,
  duration,
  release,
  tempo,
  time_signature,
  title,
  year,
  current_timestamp()
FROM
  databricks_ws_35ef6e3c_9b13_4349_a245_c6e3e49c3f11.default.raw_song_data

