-- Read all of Shakespeare's plays
CREATE TEMPORARY VIEW shakespeare
  USING parquet
  OPTIONS (path "data/shakespeare.gz.parquet");

-- Print the table schema and additional informations to the console
DESCRIBE EXTENDED shakespeare;

-- Calculate number of lines of each work and print to the console
SELECT play_name, count(line_id) AS lines
  FROM shakespeare
  GROUP BY play_name
  ORDER BY lines DESC
  LIMIT 20;

-- Create a table for the result as one file in JSON Lines format
SET spark.sql.shuffle.partitions=1;
CREATE TABLE IF NOT EXISTS lengthOfPlay (
  play_name STRING,
  lines INT )
  USING json
  LOCATION "length_of_play";

-- Save the result into the prepared table
INSERT OVERWRITE TABLE lengthOfPlay
  SELECT play_name, count(line_id) AS lines
  FROM shakespeare
  GROUP BY play_name
  ORDER BY lines DESC;
