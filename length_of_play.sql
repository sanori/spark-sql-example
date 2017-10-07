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

-- Save the result as one file in JSON Lines format
DROP TABLE IF EXISTS lengthOfPlay;  -- to overwrite, remove existing tabl
SET spark.sql.shuffle.partitions=1;  -- to make single output file
CREATE TABLE lengthOfPlay
  USING json
  LOCATION "length_of_play"
  AS SELECT play_name, count(line_id) AS lines
    FROM shakespeare
    GROUP BY play_name
    ORDER BY lines DESC;
