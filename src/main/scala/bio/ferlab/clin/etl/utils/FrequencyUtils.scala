package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object FrequencyUtils {

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)

  val frequencyFilter: Column = array_contains(col("filters"), "PASS") && col("ad_alt") >= 3 && col("gq") >= 20
  val somaticFrequencyFilter: Column = col("ad_alt") >= 2

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
  val pcNoFilter: Column = sum(when(array_contains(col("calls"), 1), 1).otherwise(0)) as "pc"
  val pcSomatic: Column = count_distinct(when(somaticFrequencyFilter, col("sample_id"))) as "pc"

  /**
   * participant total number
   */
  val pn: Column = sum(lit(1)) as "pn"
  val pnSomatic: Column = count_distinct(col("sample_id")) as "pn"
  val pnCnv: Column = count_distinct(col("aliquot_id")) as "pn"

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

  def emptyParticipantFrequency(pn: Long): Column = struct(
    lit(0) as "pc",
    lit(pn) as "pn",
    lit(0.0) as "pf"
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

  def emptyCnvGermFrequency(pn: Long): Column = struct(
    emptyParticipantFrequency(pn) as "affected",
    emptyParticipantFrequency(pn) as "non_affected",
    emptyParticipantFrequency(pn) as "total"
  )

  final val EmptyGnomadV4 = struct(
    lit(0.0).cast("double") as "sc",
    lit(0.0).cast("double") as "sn",
    lit(0.0).cast("double") as "sf"
  ) as "gnomad_exomes_4"

  final val EmptyClusterFrequencies = struct(
    EmptyGnomadV4
  ) as "external_frequencies"

  implicit class FrequencyOps(df: DataFrame)(implicit spark: SparkSession) {

    import spark.implicits._

    def getPn(pn: Column): Long = {
      df
        .select(pn)
        .as[Long]
        .collect()
        .headOption
        .getOrElse(0L)
    }

    def getPnMap(groupBy: String, pn: Column): Map[String, Long] = {
      df
        .groupBy(groupBy)
        .agg(pn)
        .as[(String, Long)]
        .collect()
        .toMap
    }

    def withSomaticFreqColumn(colName: String, filter: Column, pn: Long): DataFrame =
      df.withColumn(colName, when(filter, struct(
        $"pc",
        lit(pn) as "pn",
        $"pc" / lit(pn) as "pf"
      )))
  }
}
