package movies.etl

import movies.etl.common.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scopt.OParser

object Main extends Logging{
  case class Args(input: String = "data", output: String = "out")

  def parseArgs(argv: Array[String]): Args = {
    val builder = OParser.builder[Args]
    val parser = {
      import builder._
      OParser.sequence(
        programName("movies-etl"),
        head("Movies ETL", "0.1.0"),
        opt[String]("input").action((x, c) => c.copy(input = x)).text("input directory"),
        opt[String]("output").action((x, c) => c.copy(output = x)).text("output directory")
      )
    }
    OParser.parse(parser, argv, Args()).getOrElse(Args())
  }

  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)

    val spark = SparkSession.builder()
      .appName("movies-etl")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    logger.info(s"Initiated spark session")

    val result = MoviePipeline.buildUnified(spark, args.input)

    result.show(false) //just for checking

    logger.info(s"Writing results")
    result.repartition(col("year"))
      .write
      .mode("overwrite")
      .partitionBy("year")
      .parquet(args.output)

    spark.stop()
  }
}
