package movies.etl

import movies.etl.Main.logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import movies.etl.Normalize._
import movies.etl.RawSchemas._
import movies.etl.UnifiedColumns._
import movies.etl.common.Logging

object MoviePipeline extends Logging {

  def processCriticsProvider(spark: SparkSession, path: String): DataFrame = {
    logger.info(s"Processing critics provider")
    spark.read
      .option("header", "true")
      .schema(criticsSchema)
      .csv(s"$path/critics.csv")
      .select(
        normTitle("movie_title").as(UnifiedTitle),
        toInt("release_year").as(Year),
        col("critic_score_percentage").cast("int").as(CriticScorePct),
        col("top_critic_score").cast("double").as(TopCriticScore),
        col("total_critic_reviews_counted")
          .cast("int")
          .as(TotalCriticReviews)
      )
  }

  /** Process Audience Pulse: reads, cleans, and normalizes Provider 2 data.
    * Uses constants for output column names.
    */
  def processAudienceProvider(spark: SparkSession, path: String): DataFrame = {
    logger.info(s"Processing audience provider")
    spark.read
      .schema(audienceSchema)
      .option(
        "multiline",
        "true"
      ) // usually spark expects an specific json format
      .json(s"$path/audience/audience.json")
      .select(
        normTitle("title").as(UnifiedTitle),
        toInt("year").as(Year),
        col("audience_avg").as(AudienceAvgScore),
        col("audience_count").as(TotalAudienceRatings),
        col("domestic_gross")
          .as(DomesticGrossP2) // P2 domestic gross (to be resolved later)
      )
  }

  def processFinancials(spark: SparkSession, path: String): DataFrame = {
    logger.info(s"Processing financials providers")
    // 3a. Domestic
    val domesticDF = spark.read
      .option("header", "true")
      .schema(boxOfficeSchema)
      .csv(s"$path/box_domestic.csv")
      .select(
        normTitle("title").as(UnifiedTitle),
        toInt("year").as(Year),
        col("box_office_gross_usd")
          .as(DomesticGrossP3) // P3 domestic gross (to be resolved later)
      )

    // 3b. International
    val internationalDF = spark.read
      .option("header", "true")
      .schema(boxOfficeSchema)
      .csv(s"$path/box_international.csv")
      .select(
        normTitle("title").as(UnifiedTitle),
        toInt("year").as(Year),
        col("box_office_gross_usd").as(InternationalGross)
      )

    // 3c. Finances
    val financialsDF = spark.read
      .option("header", "true")
      .schema(provider3FinancialsSchema)
      .csv(s"$path/financials.csv")
      .select(
        normTitle("title").as(UnifiedTitle),
        toInt("year").as(Year),
        col("budget").as(ProductionBudget),
        col("marketing").as(MarketingSpend)
      )

    // Join the three sub-sources of Provider 3 using the unified keys
    domesticDF
      .join(internationalDF, Seq(UnifiedTitle, Year), "full_outer")
      .join(financialsDF, Seq(UnifiedTitle, Year), "full_outer")
  }

  /** runs the whole ETL business logic, applying governance and cleanup.
    * @param inputPath base path for directory data
    */
  def buildUnified(spark: SparkSession, inputPath: String): DataFrame = {
    logger.info(s"running business logic")

    import spark.implicits._
    val p1 = processCriticsProvider(spark, inputPath)
    val p2 = processAudienceProvider(spark, inputPath)
    val p3 = processFinancials(spark, inputPath)

    // Join all three main providers
    val combinedDF = p1
      .join(p2, Seq(UnifiedTitle, Year), "full_outer")
      .join(p3, Seq(UnifiedTitle, Year), "full_outer")

    // 1. Conflict Resolution (Governance): Coalesce P3 first (Box Office pure) over P2 (Audience Pulse)
    val unifiedGrossDF = combinedDF
      .withColumn(
        DomesticGross,
        coalesce(col(DomesticGrossP3), col(DomesticGrossP2))
      )
      .drop(
        DomesticGrossP2,
        DomesticGrossP3
      ) // Drop the source-specific columns

    // 2. Data Quality / Filtering (Example: Year must be recent/valid)
    val qualityCheckedDF = unifiedGrossDF.filter(
      col(Year).geq(
        1888
      ) and // Year must be reasonable (post-invention of cinema)
        (col(ProductionBudget).isNull or col(ProductionBudget).geq(
          0
        )) // Budget must not be negative
    )

    // 3. Add Governance Metadata
    qualityCheckedDF
      .withColumn(
        IngestionTimestamp,
        current_timestamp()
      ) // When the pipeline ran
      .select(
        // Claves
        col(UnifiedTitle),
        col(Year),
        // P1
        col(CriticScorePct),
        col(TopCriticScore),
        col(TotalCriticReviews),
        // P2
        col(AudienceAvgScore),
        col(TotalAudienceRatings),
        // P3 (Unified & Finance)
        col(DomesticGross),
        col(InternationalGross),
        col(ProductionBudget),
        col(MarketingSpend)
      )
      .as[UnifiedMovie]
      .toDF()
  }
}
