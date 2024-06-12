package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object FrequencyUtils {

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)

  val frequencyFilter: Column = array_contains(col("filters"), "PASS") && col("ad_alt") >= 3 && col("gq") >= 20
  val somaticTumorOnlyFrequencyFilter: Column = array_contains(col("filters"), "PASS") && col("ad_alt") >= 2 && col("sq") >= 30
  val somaticTumorNormalFrequencyFilter: Column = array_contains(col("filters"), "PASS") && col("ad_alt") >= 2 && col("sq") >= 30 // TODO confirm with geneticians

  /**
   * allele count
   */
  val ac: Column = sum(when(frequencyFilter, array_sum(filter(col("calls"), c => c === 1))).otherwise(0)) as "ac"

  /**
   * allele total number
   */
  val an: Column = sum(lit(2)) as "an" //alternate computation method: sum(size(col("calls"))) as "an"

  /**
   * participant count
   */
  val pc: Column = sum(when(array_contains(col("calls"), 1) and frequencyFilter, 1).otherwise(0)) as "pc"
  val pcSomaticTumorOnly: Column = count_distinct(when(somaticTumorOnlyFrequencyFilter, col("sample_id"))) as "pc"
  val pcSomaticTumorNormal: Column = count_distinct(when(somaticTumorNormalFrequencyFilter, col("sample_id"))) as "pc"

  /**
   * participant total number
   */
  val pn: Column = sum(lit(1)) as "pn"
  val pnSomatic: Column = count_distinct(col("sample_id")) as "pn"

  val hom: Column = sum(when(col("zygosity") === "HOM" and frequencyFilter, 1).otherwise(0)) as "hom"
  val het: Column = sum(when(col("zygosity") === "HET" and frequencyFilter, 1).otherwise(0)) as "het"

  val emptyFrequency =
    struct(
      lit(0L) as "ac",
      lit(0L) as "an",
      lit(0.0) as "af",
      lit(0L) as "pc",
      lit(0L) as "pn",
      lit(0.0) as "pf",
      lit(0L) as "hom"
    )

  val emptyFrequencyRQDM = struct(
    emptyFrequency as "affected",
    emptyFrequency as "non_affected",
    emptyFrequency as "total"
  )

  val emptyFrequencies = struct(
    lit("") as "analysis_display_name",
    lit("") as "analysis_code",
    emptyFrequency as "affected",
    emptyFrequency as "non_affected",
    emptyFrequency as "total"
  )

  def emptySomaticFrequency(pn: Long): Column = struct(
    lit(0) as "pc",
    lit(pn) as "pn",
    lit(0.0) as "pf"
  )

  implicit class FrequencyOps(df: DataFrame)(implicit spark: SparkSession) {

    import spark.implicits._

    def getPnSomatic(filter: Column): Long = df
      .filter(filter)
      .select("pn")
      .as[Long]
      .collect()
      .headOption
      .getOrElse(0L)

    def withSomaticFreqColumn(colName: String, filter: Column, pn: Long) =
      df.withColumn(colName, when(filter, struct(
        $"pc",
        lit(pn) as "pn",
        $"pc" / lit(pn) as "pf"
      )))
  }
}
