package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.utils.Region
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, array_union, coalesce, col, count_distinct, lit, when}

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

  def withCount(snv: DataFrame, cnv: DataFrame, countColName: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val snvRegion = Region($"snv.chromosome", $"snv.start", $"snv.end")
    val cnvRegion = Region($"cnv.chromosome", $"cnv.start", $"cnv.end")

    // condition is always the same: cnv.start <= snv.start <=cnv.end
    val countDf = snv.as("snv").join(
      cnv.alias("cnv"),
      ($"snv.sequencing_id" === $"cnv.sequencing_id") and
        ($"snv.bioinfo_analysis_code" === $"cnv.bioinfo_analysis_code") // to prevent joining tumor only data with tumor normal data
        and cnvRegion.isIncludingStartOf(snvRegion),
      "left"
    )

    countColName match {
      case "snv_count" => {
        val toJoin = countDf.groupBy("cnv.sequencing_id", "cnv.name", "cnv.bioinfo_analysis_code")
          .agg(count_distinct($"snv.hgvsg") as "count")
          .select(
            $"cnv.sequencing_id" as "sequencing_id",
            $"cnv.name" as "name",
            $"cnv.bioinfo_analysis_code" as "bioinfo_analysis_code",
            $"count",
          )

        cnv.join(toJoin, Seq("sequencing_id", "name", "bioinfo_analysis_code"), "left")
          .select(cnv("*"), coalesce($"count", lit(0)) as countColName)
      }
      case "cnv_count" => {
        val toJoin = countDf.groupBy("snv.sequencing_id", "snv.hgvsg", "snv.bioinfo_analysis_code")
          .agg(count_distinct($"cnv.name") as "count")
          .select(
            $"snv.sequencing_id" as "sequencing_id",
            $"snv.hgvsg" as "hgvsg",
            $"snv.bioinfo_analysis_code" as "bioinfo_analysis_code",
            $"count",
          )

        snv.join(toJoin, Seq("sequencing_id", "hgvsg", "bioinfo_analysis_code"), "left")
          .select(snv("*"), coalesce($"count", lit(0)) as countColName)
      }
      case _ => throw new IllegalStateException(s"Unknown col count name: $countColName expecting: [snv_count|cnv_count]")
    }
  }

}
