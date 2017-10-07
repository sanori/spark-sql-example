# Spark scripts only in SQL

SparkSQL only examples that process William Shakespeare's plays.
The text of William Shakespeare's plays are from https://old.datahub.io/dataset/william-shakespeare-plays .

## How to run

SQL scripts:

```sh
$SPARK_HOME/bin/spark-sql -f length_of_play.sql
```

Python scripts:

```sh
$SPARK_HOME/bin/spark-submit length_of_play.py
```

## References

- Spark SQL, DataFrames and Datasets Guide: <http://spark.apache.org/docs/latest/sql-programming-guide.html>
- Spark SQL Reference: <https://docs.databricks.com/spark/latest/spark-sql/index.html>
