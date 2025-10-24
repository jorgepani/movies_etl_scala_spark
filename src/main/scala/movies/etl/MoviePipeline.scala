package movies.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import movies.etl.Normalize._
import movies.etl.RawSchemas._

object MoviePipeline {

  /** Process Critics: cleans and normalizes.
    */
  def processCriticsProvider(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(criticsSchema)
      .csv(s"$path/critics.csv")
      .select(
        normTitle("movie_title").as("unified_title"),
        toInt("release_year").as("year"),
        col("critic_score_percentage").cast("int").as("critic_score_pct"),
        col("top_critic_score").cast("double").as("top_critic_score"),
        col("total_critic_reviews_counted")
          .cast("int")
          .as("total_critic_reviews")
      )
  }

  /** Process Audience Pulse
    */
  def processAudienceProvider(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .schema(audienceSchema)
      .option("multiline", "true") //usually spark expects an specific json format
      .json(s"$path/audience/audience.json")
      .select(
        normTitle("title").as("unified_title"),
        toInt("year").as("year"),
        col("audience_avg").as("audience_avg_score"),
        col("audience_count").as("total_audience_ratings"),
        col("domestic_gross").as("domestic_gross_p2")
      )
  }

  /** Process BoxOfficeMetrics: reads and joins 3 sources.
    */
  def processFinancials(spark: SparkSession, path: String): DataFrame = {
    // 3a. Domestic
    val domesticDF = spark.read
      .option("header", "true")
      .schema(boxOfficeSchema)
      .csv(s"$path/box_domestic.csv")
      .select(
        normTitle("film_name").as("unified_title"),
        toInt("year_of_release").as("year"),
        col("box_office_gross_usd").as("domestic_gross_p3")
      )

    // 3b. International
    val internationalDF = spark.read
      .option("header", "true")
      .schema(boxOfficeSchema)
      .csv(s"$path/box_international.csv")
      .select(
        normTitle("film_name").as("unified_title"),
        toInt("year_of_release").as("year"),
        col("box_office_gross_usd").as("international_gross")
      )

    // 3c. Finances
    val financialsDF = spark.read
      .option("header", "true")
      .schema(provider3FinancialsSchema)
      .csv(s"$path/financials.csv")
      .select(
        normTitle("film_name").as("unified_title"),
        toInt("year_of_release").as("year"),
        col("production_budget_usd").as("production_budget"),
        col("marketing_spend_usd").as("marketing_spend")
      )

    domesticDF
      .join(internationalDF, Seq("unified_title", "year"), "full_outer")
      .join(financialsDF, Seq("unified_title", "year"), "full_outer")
  }

  /** runs the whole ETL
    * @param inputPath base path for directory data
    */
  def buildUnified(spark: SparkSession, inputPath: String): DataFrame = {
    import spark.implicits._
    val p1 = processCriticsProvider(spark, inputPath)
    val p2 = processAudienceProvider(spark, inputPath)
    val p3 = processFinancials(spark, inputPath)

    val combinedDF = p1
      .join(p2, Seq("unified_title", "year"), "full_outer")
      .join(p3, Seq("unified_title", "year"), "full_outer") // ¡AÑADIDO p3 aquí!

    val unifiedGrossDF = combinedDF
      .withColumn(
        "domestic_gross",
        coalesce(col("domestic_gross_p2"), col("domestic_gross_p3"))
      )
      .drop(
        "domestic_gross_p2",
        "domestic_gross_p3"
      )

    unifiedGrossDF
      .select(
        // Claves
        col("unified_title"),
        col("year"),
        // P1
        col("critic_score_pct"),
        col("top_critic_score"),
        col("total_critic_reviews"),
        // P2
        col("audience_avg_score"),
        col("total_audience_ratings"),
        col("domestic_gross"), // Col unified
        col("international_gross"),
        col("production_budget"),
        col("marketing_spend")
      )
      .as[UnifiedMovie]
      .toDF()
  }
}
