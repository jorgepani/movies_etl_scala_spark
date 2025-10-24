package movies.etl

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import movies.etl.Normalize._

import scala.concurrent.duration.Duration

class NormalizeSuite extends FunSuite {

  override val munitTimeout = Duration(1, "minutes")

  // Sesión de Spark para ejecutar la UDF en un contexto de columna
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("NormalizeTest")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  test("normTitleUdf should clean and normalize titles") {
    val rawTitles = Seq(
      "Inception",
      "The Dark Knight,",
      "Parasite (2019)",
      "Movie - With - Spaces",
      null
    ).toDF("raw_title")

    val expectedTitles = Seq(
      "inception",
      "the dark knight",
      "parasite 2019",
      "movie with spaces",
      null
    ).toDF("expected_title")

    val resultDF = rawTitles.withColumn("normalized", normTitle("raw_title"))

    // Comprobar la UDF aplicada en columna
    val result = resultDF.select("normalized").as[String].collect()
    val expected = expectedTitles.select("expected_title").as[String].collect()

    // Usar la función de comparación de Munit
    assertEquals(result.toSeq, expected.toSeq)
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}