package movies.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Normalize {
  private val normRegex = """[^\p{L}\p{N}\s]"""

  val normTitleUdf = udf { s: String =>
    if (s == null) null
    else {
      s.toLowerCase.trim
        .replaceAll(normRegex, "")
        .replaceAll("""\s+""", " ")
    }
  }

  def normTitle(colName: String): Column = normTitleUdf(col(colName))
  def toInt(colName: String): Column = regexp_extract(col(colName), """(\d+)""", 1).cast("int")
}
