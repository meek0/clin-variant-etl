package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.enriched.CNV.CnvRegion
import bio.ferlab.clin.etl.utils.Region
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, array_union, col, count, lit, when}

package object enriched {

  def withExternalReference(df: DataFrame, conditionValueMap: List[(Column, String)], outputColumn: String = "variant_external_reference"): DataFrame = {
    conditionValueMap
      .tail
      .foldLeft(
        df.withColumn(outputColumn, when(conditionValueMap.head._1, array(lit(conditionValueMap.head._2))).otherwise(array()))
      ) { case (currDf, (cond, value)) =>
        currDf.withColumn(outputColumn, when(cond, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
  }

  def withCount(left: DataFrame, leftColToGroup: String, right: DataFrame, rightColToCount: String, countColName: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val leftRegion = Region($"left.chromosome", $"left.start", $"left.end")
    val rightRegion = Region($"right.chromosome", $"right.start", $"right.end")

    val countDf = left.as("left").join(right.alias("right"), ($"left.service_request_id" === $"right.service_request_id") and leftRegion.isIncludingStartOf(rightRegion), "left")
      .groupBy("left.service_request_id", s"left.$leftColToGroup")
      .agg(count(right(rightColToCount)) as "count")
      .select(
        $"left.service_request_id" as "service_request_id",
        left(leftColToGroup) as leftColToGroup,
        $"count",
      )

    left.join(countDf, Seq("service_request_id", leftColToGroup), "left")
      .select(left("*"), $"count" as countColName)
  }

}
