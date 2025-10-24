# Movies ETL — Scala + Spark

Example ETL pipeline in Scala + Apache Spark. It reads data from 3 providers (CSV/JSON), normalizes join keys, performs joins, and writes Parquet partitioned by year. Includes tests and an example DAG for Airflow (using SparkSubmitOperator).
I decided to use Scala because it's the base framework for databricks and I'm used to it and the test was language agnostic
To show I'm used to use Python I'll rewrite the code to use Pyspark, Pandas and argparse

## Requisites
- Java 11+
- sbt 1.9+
- Spark 3.5.x (para `spark-submit`)

## Estructure
```
movies_etl_scala_spark/
  build.sbt
  project/plugins.sbt
  src/
    main/scala/movies/etl/{Main.scala, MoviePipeline.scala, Normalize.scala, models.scala}
    test/scala/movies/etl/PipelineSuite.scala, NormalizeSuite.scala
  dags/movies_etl_scala_spark.py
```

## Tests
```bash
sbt test
```

## local run (sbt)
```bash
sbt "run --input src/test/resources/data --output out"
```
Arguments:
```
--input data --output out
```

Running locally could show fake errors while loading the spark context depending on the java version but in less than 20 seconds 
a completely normal log will appear showing the process and the dataframe show

[info] +---------------+----+----------------+----------------+--------------------+------------------+----------------------+--------------+-------------------+-----------------+---------------+
[info] |unified_title  |year|critic_score_pct|top_critic_score|total_critic_reviews|audience_avg_score|total_audience_ratings|domestic_gross|international_gross|production_budget|marketing_spend|
[info] +---------------+----+----------------+----------------+--------------------+------------------+----------------------+--------------+-------------------+-----------------+---------------+
[info] |inception      |2010|87              |8.2             |200                 |9.1               |1500000               |292576195     |535000000          |160000000        |100000000      |
[info] |parasite       |2019|96              |9.0             |180                 |8.6               |800000                |53000000      |205000000          |11400000         |8000000        |
[info] |the dark knight|2008|94              |8.8             |250                 |9.0               |2500000               |535000000     |469000000          |185000000        |120000000      |
[info] +---------------+----+----------------+----------------+--------------------+------------------+----------------------+--------------+-------------------+-----------------+---------------+

## Packaging JAR (fat-jar) to run with spark-submit
```bash
sbt assembly
spark-submit --master local[*]   --class movies.etl.Main   target/scala-2.12/movies-etl_2.12-0.1.0.jar   --input data --output out_spark
```

## Notes
- By default, it's writing in parquet, we can move to delta just by... `format("delta")`.
- Normalizer is just a simple UDF: lower case, special chars, several spaces to just one.
