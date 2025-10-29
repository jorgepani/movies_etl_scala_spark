package movies.etl

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.{LogManager, Logger}

import java.io.File
import scala.concurrent.duration.Duration

class PipelineSuite extends FunSuite {



  override val munitTimeout = Duration(1, "minutes")

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("PipelineTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  test("reads and maps audience PULSE data correctly") {
    val inputPath = getInputPath()

    val df = MoviePipeline.processAudienceProvider(spark, inputPath)

    assertEquals(df.count(), 3L, "Row count for critics must be 3")

    val inceptionRow =
      df.filter($"unified_title" === "inception" and $"year" === 2010).head()

    assertEquals(
      inceptionRow.getAs[Long]("total_audience_ratings"),
      1500000L,
      "Ratings must match JSON data and be mapped to total_audience_ratings."
    )

    assertEquals(
      inceptionRow.getAs[Long]("domestic_gross_p2"),
      292576195L,
      "Domestic gross must be mapped to domestic_gross_p2."
    )
  }

  test("reads and maps CriticAgg data correctly") {
    val inputPath = getInputPath()

    val df = MoviePipeline.processCriticsProvider(spark, inputPath)

    assertEquals(df.count(), 3L, "Row count for critics must be 3")

    val inceptionRow =
      df.filter($"unified_title" === "inception" and $"year" === 2010).head()

    assertEquals(
      inceptionRow.getAs[Int]("critic_score_pct"),
      87,
      "Critic score for Inception must be 87 (Int)"
    )
  }

  test(
    "process financial provider reads and joins BoxOfficeMetrics data correctly"
  ) {
    val inputPath = getInputPath()

    val df = MoviePipeline.processFinancials(spark, inputPath)

    assertEquals(
      df.count(),
      3L,
      "Row count for Provider 3 must be 3 unique movies."
    )

    val inceptionRow =
      df.filter($"unified_title" === "inception" and $"year" === 2010).head()

    df.show(15)

    assertEquals(
      inceptionRow.getAs[Long]("domestic_gross_p3"),
      292000000L,
      "Domestic Gross P3 must be present."
    )
    assertEquals(
      inceptionRow.getAs[Long]("international_gross"),
      535000000L,
      "International Gross must be present."
    )
    assertEquals(
      inceptionRow.getAs[Long]("production_budget"),
      160000000L,
      "Production Budget must be present."
    )
  }

  test(
    "buildUnified should process all providers and produce correct unified view"
  ) {
    val inputPath = getInputPath()

    val unifiedDF = MoviePipeline.buildUnified(spark, inputPath)

    unifiedDF.show(false)

    val expectedSchema = spark
      .createDataset(
        Seq(
          UnifiedMovie(
            "",
            Some(0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None
          )
        )
      )
      .schema
    assertEquals(unifiedDF.schema, expectedSchema)

    val inceptionRow = unifiedDF
      .filter($"unified_title" === "inception" and $"year" === 2010)
      .head()

    assertEquals(inceptionRow.getAs[Int]("critic_score_pct"), 87)

    assertEquals(inceptionRow.getAs[Long]("total_audience_ratings"), 1500000L)

    assertEquals(
      inceptionRow.getAs[Long]("domestic_gross"),
      292000000L
    )

    assertEquals(inceptionRow.getAs[Long]("international_gross"), 535000000L)
    assertEquals(inceptionRow.getAs[Long]("production_budget"), 160000000L)

    assertEquals(unifiedDF.count(), 3L)
  }

  def getInputPath(): String = {
    val projectRoot = System.getProperty("user.dir")

    val inputPath = Seq(
      projectRoot,
      "src",
      "test",
      "resources",
      "data"
    ).mkString(File.separator)

    val inputDir = new File(inputPath)
    if (!inputDir.exists() || !inputDir.isDirectory) {
      fail(
        s"¡Error crítico en el path de prueba! Directorio de entrada no encontrado en: $inputPath"
      )
    }
    inputPath
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
