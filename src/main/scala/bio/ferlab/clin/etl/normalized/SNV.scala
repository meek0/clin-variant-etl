package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.normalized.SNV._
import bio.ferlab.clin.etl.utils.FrequencyUtils.includeFilter
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.ParentalOrigin.{FTH, MTH}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.utils.RepartitionByColumns
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}

import java.time.LocalDateTime

class SNV(batchId: String)(implicit configuration: Configuration) extends Occurrences(batchId) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_snv")
  override val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val rare_variants: DatasetConf = conf.getDataset("enriched_rare_variant")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    super.extract(lastRunDateTime, currentRunDateTime) + (rare_variants.id -> rare_variants.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val joinedRelation: DataFrame = getClinicalRelation(data)

    val occurrences = getSNV(data(raw_variant_calling.id), batchId)
      .join(broadcast(joinedRelation), Seq("aliquot_id"), "inner")
      .withColumn("participant_id", col("patient_id"))
      .withColumn("family_info", familyInfo(
        Seq(
          col("gq"), col("dp"), col("qd"), col("filters"),
          col("ad_ref"), col("ad_alt"), col("ad_total"), col("ad_ratio"),
          col("calls"), col("affected_status")))
      )
      .withColumn("mother_calls", motherCalls)
      .withColumn("father_calls", fatherCalls)
      .withColumn("mother_affected_status", motherAffectedStatus)
      .withColumn("father_affected_status", fatherAffectedStatus)
      .withColumn("mother_gq", motherGQ)
      .withColumn("mother_dp", motherDP)
      .withColumn("mother_qd", motherQD)
      .withColumn("mother_filters", motherFilters)
      .withColumn("mother_ad_ref", motherADRef)
      .withColumn("mother_ad_alt", motherADAlt)
      .withColumn("mother_ad_total", motherADTotal)
      .withColumn("mother_ad_ratio", motherADRatio)
      .withColumn("father_gq", fatherGQ)
      .withColumn("father_dp", fatherDP)
      .withColumn("father_qd", fatherQD)
      .withColumn("father_filters", fatherFilters)
      .withColumn("father_ad_ref", fatherADRef)
      .withColumn("father_ad_alt", fatherADAlt)
      .withColumn("father_ad_total", fatherADTotal)
      .withColumn("father_ad_ratio", fatherADRatio)
      .drop("family_info", "participant_id")
      .withColumn("zygosity", zygosity(col("calls")))
      .withColumn("mother_zygosity", zygosity(col("mother_calls")))
      .withColumn("father_zygosity", zygosity(col("father_calls")))
      .withParentalOrigin("parental_origin", col("calls"), col("father_calls"), col("mother_calls"))
      .withGenotypeTransmission("transmission")
      .filter(col("has_alt"))
      .persist()

    val hcFilter = col("is_rare") and col("ad_alt") > 2 and col("gq") >= 20
    addRareVariantColumn(occurrences, data(rare_variants.id))
        .withCompoundHeterozygous(additionalFilter = Some(hcFilter))
        .drop("symbols", "is_rare")

  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(Seq("chromosome"), Some(100))

  override def replaceWhere: Option[String] = Some(s"batch_id = '$batchId'")
}

object SNV {

  def getSNV(inputDf: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    inputDf
      .withColumn("genotype", explode(col("genotypes")))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        csq,
        firstCsq,
        $"genotype.sampleId" as "aliquot_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        $"INFO_QD" as "qd",
        array_contains($"genotype.calls", 1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic,
        flatten(transform($"INFO_FILTERS", c => split(c, ";"))) as "filters"
      )
      .withColumn("symbols", array_distinct($"annotations.symbol"))
      .drop("annotations")
      .withColumn("ad_ref", $"ad"(0))
      .withColumn("ad_alt", $"ad"(1))
      .withColumn("ad_total", $"ad_ref" + $"ad_alt")
      .withColumn("ad_ratio", when($"ad_total" === 0, 0).otherwise($"ad_alt" / $"ad_total"))
      .drop("ad")
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_update", current_date())
      .withColumn("variant_type", lit("germline"))
      .filter($"alternate" =!= "*")
      .filter(includeFilter)
      .drop("annotation")
  }

  def addRareVariantColumn(occurrences: DataFrame, rareVariants: DataFrame): DataFrame = {
    occurrences
      .joinByLocus(rareVariants, "left")
      .withColumn("is_rare", coalesce(col("is_rare"), lit(true))) // if a variant is not found into table rare_variant then it's a rare variant
  }


}