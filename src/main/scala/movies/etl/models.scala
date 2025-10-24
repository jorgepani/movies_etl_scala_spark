package movies.etl

import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  DoubleType,
  LongType,
  IntegerType
}

// ensuring we read exactly the types we want
object RawSchemas {
  // Provider 1: CriticAgg (CSV)
  val criticsSchema: StructType = StructType(
    Array(
      StructField("movie_title", StringType, nullable = true),
      StructField("release_year", StringType, nullable = true),
      StructField("critic_score_percentage", StringType, nullable = true),
      StructField("top_critic_score", StringType, nullable = true),
      StructField("total_critic_reviews_counted", StringType, nullable = true)
    )
  )

  // Provider 2: Audience Pulse (JSON) - Spark makes some inference but we force the data with the schema
  val audienceSchema: StructType = StructType(
    Array(
      StructField("title", StringType, nullable = true),
      StructField("year", StringType, nullable = true),
      StructField("audience_avg", DoubleType, nullable = true),
      StructField("audience_count", LongType, nullable = true),
      StructField("domestic_gross", LongType, nullable = true)
    )
  )

  // Provider 3: BoxOfficeMetrics
  val boxOfficeSchema: StructType = StructType(
    Array(
      StructField("film_name", StringType, nullable = true),
      StructField("year_of_release", StringType, nullable = true),
      StructField("box_office_gross_usd", LongType, nullable = true)
    )
  )

  // Provider 3: Financials
  val provider3FinancialsSchema: StructType = StructType(
    Array(
      StructField("film_name", StringType, nullable = true),
      StructField("year_of_release", StringType, nullable = true),
      StructField("production_budget_usd", LongType, nullable = true),
      StructField("marketing_spend_usd", LongType, nullable = true)
    )
  )
}

// Unified final model
case class UnifiedMovie(
    // Keys (standard)
    unified_title: String,
    year: Option[Int],
    // Critics (Provider 1)
    critic_score_pct: Option[Int],
    top_critic_score: Option[Double],
    total_critic_reviews: Option[Int],
    // Audience (Provider 2)
    audience_avg_score: Option[Double],
    total_audience_ratings: Option[Long],
    // Finance (Provider 2 and 3)
    domestic_gross: Option[Long],
    international_gross: Option[Long],
    production_budget: Option[Long],
    marketing_spend: Option[Long]
)
