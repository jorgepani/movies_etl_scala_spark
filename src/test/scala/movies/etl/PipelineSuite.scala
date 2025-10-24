package movies.etl

import munit.FunSuite
import org.apache.spark.sql.SparkSession

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

    // 4. Verificar Mapeo y Tipado del conteo de audiencia (el valor real de tu JSON)
    // El valor 1,200,000L es el valor que encontramos en tu JSON real para 'Inception'.
    assertEquals(
      inceptionRow.getAs[Long]("total_audience_ratings"),
      1500000L,
      "Ratings must match JSON data and be mapped to total_audience_ratings."
    )

    // 5. Verificar Mapeo y Tipado del gross (el valor real de tu JSON)
    // domestic_gross en el JSON debe ser mapeado a domestic_gross_p2
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

    // 1. Verificar Conteo de Filas (Asumiendo 3 registros únicos después de los full_outer joins internos)
    assertEquals(
      df.count(),
      3L,
      "Row count for Provider 3 must be 3 unique movies."
    )

    // 2. Verificar Combianción de Métricas para 'Inception'
    val inceptionRow =
      df.filter($"unified_title" === "inception" and $"year" === 2010).head()

    df.show(15)

    // Verificar que los datos de los 3 archivos se unieron correctamente
    // Nota: El valor de 'domestic_gross_p3' se asume de un archivo de prueba.
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
    // ASUMO que tienes archivos de prueba en src/test/resources/data/

    val projectRoot = new File(".").getAbsolutePath

    // 2. Construir la ruta completa al directorio de entrada 'data'
    // El separador de archivos (File.separator) asegura compatibilidad con Windows/Linux
    val inputPath = getInputPath()

    // 1. EJECUCIÓN: Obtener el DataFrame unificado
    val unifiedDF = MoviePipeline.buildUnified(spark, inputPath)

    unifiedDF.show(false)

    // 2. VERIFICACIÓN 1: Esquema y Tipo
    // Se verifica que el esquema del DF final coincida con el case class UnifiedMovie
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

    // 3. VERIFICACIÓN 2: Contenido (Usando la película 'Inception' como ejemplo)
    val inceptionRow = unifiedDF
      .filter($"unified_title" === "inception" and $"year" === 2010)
      .head()

    // Verificar métricas de P1
    assertEquals(inceptionRow.getAs[Int]("critic_score_pct"), 87)

    // Verificar métricas de P2
    assertEquals(inceptionRow.getAs[Long]("total_audience_ratings"), 1500000L)

    // Verificar métricas de P2/P3 (Domestic Gross)
    assertEquals(
      inceptionRow.getAs[Long]("domestic_gross"),
      292576195L
    ) // Tomado de P2 o P3

    // Verificar métricas de P3 (International Gross y Budget)
    assertEquals(inceptionRow.getAs[Long]("international_gross"), 535000000L)
    assertEquals(inceptionRow.getAs[Long]("production_budget"), 160000000L)

    // 4. VERIFICACIÓN 3: Conteo total de filas
    // Asumiendo que los datos de prueba contienen 3 películas únicas
    assertEquals(unifiedDF.count(), 3L)
  }

  def getInputPath(): String = {
    // 1. Obtener la ruta absoluta del directorio base del proyecto
    val projectRoot = System.getProperty("user.dir")

    // 2. Construir la ruta completa al directorio de entrada 'data'
    val inputPath = Seq(
      projectRoot,
      "src",
      "test",
      "resources",
      "data"
    ).mkString(File.separator)

    // 3. Verificación de existencia para un error claro
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
