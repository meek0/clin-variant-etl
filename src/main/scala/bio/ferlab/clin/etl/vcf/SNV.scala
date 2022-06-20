package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.FrequencyUtils.includeFilter
import bio.ferlab.clin.etl.vcf.SNV.{getCompoundHet, getPossiblyCompoundHet, getSNV}
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class SNV(batchId: String)(implicit configuration: Configuration) extends Occurrences(batchId) {

  override val destination: DatasetConf = conf.getDataset("normalized_snv")
  override val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val joinedRelation: DataFrame = getClinicalRelation(data)

    val occurrences = getSNV(data(raw_variant_calling.id), batchId)
      .join(joinedRelation, Seq("aliquot_id"), "inner")
      .withColumn("participant_id", col("patient_id"))
      .withColumn("family_info", familyInfo)
      .withColumn("mother_calls", motherCalls)
      .withColumn("father_calls", fatherCalls)
      .withColumn("mother_affected_status", motherAffectedStatus)
      .withColumn("father_affected_status", fatherAffectedStatus)
      .withColumn("mother_gq", motherGQ)
      .withColumn("father_gq", fatherGQ)
      .drop("family_info", "participant_id")
      .withColumn("zygosity", zygosity(col("calls")))
      .withColumn("mother_zygosity", zygosity(col("mother_calls")))
      .withColumn("father_zygosity", zygosity(col("father_calls")))
      .withParentalOrigin("parental_origin", col("father_calls"), col("mother_calls"))
      .withGenotypeTransmission("transmission")
      .filter(col("has_alt"))

    val het = occurrences.filter(col("zygosity") === "HET")
    val hc: DataFrame = getCompoundHet(het)
    val possiblyHC: DataFrame = getPossiblyCompoundHet(het)
    occurrences
      .drop("symbols")
      .join(hc, Seq("chromosome", "start", "reference", "alternate", "patient_id"), "left")
      .join(possiblyHC, Seq("chromosome", "start", "reference", "alternate", "patient_id"), "left")
      .withColumn("is_hc", coalesce(col("is_hc"), lit(false)))
      .withColumn("hc_complement", coalesce(col("hc_complement"), array()))
      .withColumn("is_possibly_hc", coalesce(col("is_possibly_hc"), lit(false)))
      .withColumn("possibly_hc_complement", coalesce(col("possibly_hc_complement"), array()))
  }


  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(100, col("chromosome"))
    )
  }
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
      .filter(includeFilter)
      .drop("annotation")
  }


  def getPossiblyCompoundHet(het: DataFrame): DataFrame = {
    val hcWindow = Window.partitionBy("patient_id", "symbol").orderBy("start").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val possiblyHC = het
      .select(col("patient_id"), col("chromosome"), col("start"), col("reference"), col("alternate"), explode(col("symbols")) as "symbol")
      .withColumn("possibly_hc_count", count(lit(1)).over(hcWindow))
      .filter(col("possibly_hc_count") > 1)
      .withColumn("possibly_hc_complement", struct(col("symbol") as "symbol", col("possibly_hc_count") as "count"))
      .groupBy(col("patient_id") :: locus: _*)
      .agg(collect_set("possibly_hc_complement") as "possibly_hc_complement")
      .withColumn("is_possibly_hc", lit(true))
    possiblyHC
  }

  def getCompoundHet(het: DataFrame): DataFrame = {
    val withParentalOrigin = het.filter(col("parental_origin").isNotNull)

    val hcWindow = Window.partitionBy("patient_id", "symbol", "parental_origin").orderBy("start")
    val hc = withParentalOrigin
      .select(col("patient_id"), col("chromosome"), col("start"), col("reference"), col("alternate"), col("symbols"), col("parental_origin"))
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("symbol", explode(col("symbols")))
      .withColumn("coords", collect_set(col("locus")).over(hcWindow))
      .withColumn("merged_coords", last("coords", ignoreNulls = true).over(hcWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
      .withColumn("struct_coords", struct(col("parental_origin"), col("merged_coords").alias("coords")))
      .withColumn("all_coords", collect_set("struct_coords").over(Window.partitionBy("patient_id", "symbol").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
      .withColumn("complement_coords", functions.filter(col("all_coords"), x => x.getItem("parental_origin") =!= col("parental_origin"))(0))
      .withColumn("is_hc", col("complement_coords").isNotNull)
      .filter(col("is_hc"))
      .withColumn("hc_complement", struct(col("symbol") as "symbol", col("complement_coords.coords") as "locus"))
      .groupBy(col("patient_id") :: locus: _*)
      .agg(first("is_hc") as "is_hc", collect_set("hc_complement") as "hc_complement")
    hc
  }
}