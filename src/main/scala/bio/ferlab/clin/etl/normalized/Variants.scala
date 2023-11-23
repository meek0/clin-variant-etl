package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.normalized.Occurrences.getDiseaseStatus
import bio.ferlab.clin.etl.normalized.Variants.hotspot
import bio.ferlab.clin.etl.utils.FrequencyUtils
import bio.ferlab.clin.etl.utils.FrequencyUtils.{emptyFrequencies, emptyFrequency, emptyFrequencyRQDM}
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext, RepartitionByColumns}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.{GenomicOperations, vcf}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.slf4j.Logger

import java.time.LocalDateTime

case class Variants(rc: DeprecatedRuntimeETLContext, batchId: String) extends SingleETL(rc) {

  import spark.implicits._

  implicit val logger: Logger = log

  override val mainDestination: DatasetConf = conf.getDataset("normalized_variants")
  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val raw_variant_calling_somatic_tumor_only: DatasetConf = conf.getDataset("raw_snv_somatic_tumor_only")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), None, optional = true),
      raw_variant_calling_somatic_tumor_only.id -> vcf(raw_variant_calling_somatic_tumor_only.location.replace("{{BATCH_ID}}", batchId), None, optional = true),
      clinical_impression.id -> clinical_impression.read,
      observation.id -> observation.read,
      task.id -> task.read,
      service_request.id -> service_request.read
    )
  }

  private def getVCF(data: Map[String, DataFrame]): (DataFrame, String, String, Boolean) = {

    val vcfGermline = data(raw_variant_calling.id)
    val vcfSomaticTumorOnly = data(raw_variant_calling_somatic_tumor_only.id)

    if (!vcfGermline.isEmpty) {
      (vcfGermline.where(col("contigName").isin(validContigNames: _*)), "genotype.conditionalQuality", "gq", true)
    } else if (!vcfSomaticTumorOnly.isEmpty) {
      (vcfSomaticTumorOnly.where(col("contigName").isin(validContigNames: _*)), "genotype.SQ", "sq", false)
    } else {
      throw new Exception("Not valid raw VCF available")
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {

    val (inputVCF, srcScoreColumn, dstScoreColumn, computeFrequencies) = getVCF(data)

    val variants = getVariants(inputVCF)
    val clinicalInfos = getClinicalInfo(data)
    val variantsWithClinicalInfo = getVariantsWithClinicalInfo(inputVCF, clinicalInfos, srcScoreColumn, dstScoreColumn)

    val res = getVariantsWithFrequencies(variantsWithClinicalInfo, computeFrequencies)
      .joinByLocus(variants, "right")
      .withColumn("frequencies_by_analysis", coalesce(col("frequencies_by_analysis"), array(emptyFrequencies)))
      .withColumn("frequency_RQDM", coalesce(col("frequency_RQDM"), emptyFrequencyRQDM))
      .withColumn("batch_id", lit(batchId))
      .withColumn("created_on", current_timestamp())

    res
  }

  def getVariants(vcf: DataFrame): DataFrame = {
    vcf
      .withColumn("annotation", firstCsq)
      .withColumn("alleleDepths", functions.transform(col("genotypes.alleleDepths"), c => c(1)))
      .filter(size(filter(col("alleleDepths"), ad => ad >= 3)) > 0)
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        array_distinct(csq("symbol")) as "genes_symbol",
        hgvsg,
        variant_class,
        pubmed,
        hotspot(vcf)
      )
      .drop("annotation")
      .dropDuplicates("chromosome", "start", "reference", "alternate")
  }

  def getVariantsWithClinicalInfo(vcf: DataFrame, clinicalInfos: DataFrame, srcScoreColumn: String, dstScoreColumn: String): DataFrame = {
    vcf
      .withColumn("annotation", firstCsq)
      .select(
        explode(col("genotypes")) as "genotype",
        chromosome,
        start,
        reference,
        alternate,
        flatten(functions.transform(col("INFO_FILTERS"), c => split(c, ";"))) as "filters"
      )
      .withColumn("ad", col("genotype.alleleDepths"))
      .withColumn("ad_alt", col("ad")(1))
      .withColumn(dstScoreColumn, col(srcScoreColumn))
      .withColumn("aliquot_id", col("genotype.sampleId"))
      .withColumn("calls", col("genotype.calls"))
      .withColumn("zygosity", zygosity(col("calls")))
      .drop("genotype")
      .join(broadcast(clinicalInfos), Seq("aliquot_id"))
  }

  def getVariantsEmptyFrequencies(variants: DataFrame): DataFrame = {
    variants
      .selectLocus()
      .dropDuplicates("chromosome", "start", "reference", "alternate")
      .withColumn("frequencies_by_analysis", lit(null))
      .withColumn("frequency_RQDM", lit(null))
  }

  def getVariantsWithFrequencies(variants: DataFrame, computeFrequency: Boolean): DataFrame = {

    if (!computeFrequency)
      return getVariantsEmptyFrequencies(variants)

    val frequency: String => Column = {
      case "" =>
        struct(
          $"ac",
          $"an",
          coalesce($"ac" / $"an", lit(0.0)) as "af",
          $"pc",
          $"pn",
          coalesce($"pc" / $"pn", lit(0.0)) as "pf",
          $"hom"
        )
      case prefix: String =>
        struct(
          col(s"${prefix}_ac") as "ac",
          col(s"${prefix}_an") as "an",
          coalesce(col(s"${prefix}_ac") / col(s"${prefix}_an"), lit(0.0)) as "af",
          col(s"${prefix}_pc") as "pc",
          col(s"${prefix}_pn") as "pn",
          coalesce(col(s"${prefix}_pc") / col(s"${prefix}_pn"), lit(0.0)) as "pf",
          col(s"${prefix}_hom") as "hom"
        )
    }

    variants
      .groupBy(locus :+ col("analysis_code") :+ col("affected_status_str"): _*)
      .agg(
        first($"analysis_display_name") as "analysis_display_name",
        first($"affected_status") as "affected_status",
        FrequencyUtils.ac,
        FrequencyUtils.an,
        FrequencyUtils.het,
        FrequencyUtils.hom,
        FrequencyUtils.pc,
        FrequencyUtils.pn,
        first(struct(variants("*"))) as "variant")
      .withColumn("frequency_by_status", frequency(""))
      .groupBy(locus :+ col("analysis_code"): _*)
      .agg(
        map_from_entries(collect_list(struct($"affected_status_str", $"frequency_by_status"))) as "frequency_by_status",

        sum(when($"affected_status", $"ac").otherwise(0)) as "affected_ac",
        sum(when($"affected_status", $"an").otherwise(0)) as "affected_an",
        sum(when($"affected_status", $"pc").otherwise(0)) as "affected_pc",
        sum(when($"affected_status", $"pn").otherwise(0)) as "affected_pn",
        sum(when($"affected_status", $"hom").otherwise(0)) as "affected_hom",

        sum(when($"affected_status", 0).otherwise($"ac")) as "non_affected_ac",
        sum(when($"affected_status", 0).otherwise($"an")) as "non_affected_an",
        sum(when($"affected_status", 0).otherwise($"pc")) as "non_affected_pc",
        sum(when($"affected_status", 0).otherwise($"pn")) as "non_affected_pn",
        sum(when($"affected_status", 0).otherwise($"hom")) as "non_affected_hom",

        sum($"ac") as "ac",
        sum($"an") as "an",
        sum($"pc") as "pc",
        sum($"pn") as "pn",
        sum($"hom") as "hom",
        first($"analysis_display_name") as "analysis_display_name",
        first(col("variant")) as "variant"
      )
      .withColumn("frequency_by_status_total", map_from_entries(array(struct(lit("total"), frequency("")))))
      .withColumn("frequency_by_status", map_concat($"frequency_by_status_total", $"frequency_by_status"))
      .withColumn("frequency_by_status", when(array_contains(map_keys($"frequency_by_status"), "non_affected"), $"frequency_by_status")
        .otherwise(map_concat($"frequency_by_status", map_from_entries(array(struct(lit("non_affected"), emptyFrequency))))))
      .groupBy(locus: _*)
      .agg(
        collect_list(struct(
          $"analysis_display_name" as "analysis_display_name",
          $"analysis_code" as "analysis_code",
          col("frequency_by_status")("affected") as "affected",
          col("frequency_by_status")("non_affected") as "non_affected",
          col("frequency_by_status")("total") as "total"
        )) as "frequencies_by_analysis",
        sum($"affected_ac") as "affected_ac",
        sum($"affected_an") as "affected_an",
        sum($"affected_pc") as "affected_pc",
        sum($"affected_pn") as "affected_pn",
        sum($"affected_hom") as "affected_hom",

        sum($"non_affected_ac") as "non_affected_ac",
        sum($"non_affected_an") as "non_affected_an",
        sum($"non_affected_pc") as "non_affected_pc",
        sum($"non_affected_pn") as "non_affected_pn",
        sum($"non_affected_hom") as "non_affected_hom",

        sum($"ac") as "ac",
        sum($"an") as "an",
        sum($"pc") as "pc",
        sum($"pn") as "pn",
        sum($"hom") as "hom",
        first(col("variant")) as "variant"
      )
      .withColumn("frequency_RQDM", struct(
        frequency("affected") as "affected",
        frequency("non_affected") as "non_affected",
        frequency("") as "total"
      ))
      .select(locus :+ $"frequencies_by_analysis" :+ $"frequency_RQDM": _*)

  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(10), sortColumns = Seq("start"))

  override def replaceWhere: Option[String] = Some(s"batch_id = '$batchId'")

  def getClinicalInfo(data: Map[String, DataFrame]): DataFrame = {
    val serviceRequestDf = data(service_request.id)
      .filter(col("service_request_type") === "sequencing")
      .select(
        col("id") as "service_request_id",
        col("service_request_code") as "analysis_code",
        col("service_request_description") as "analysis_display_name",
        col("analysis_service_request_id")
      )

    val analysisServiceRequestDf = data(service_request.id)
      .filter(col("service_request_type") === "analysis")

    val analysisServiceRequestWithDiseaseStatus = getDiseaseStatus(analysisServiceRequestDf, data(clinical_impression.id), data(observation.id))
      .select("analysis_service_request_id", "patient_id", "affected_status")
      .withColumn("affected_status_str", when(col("affected_status"), lit("affected")).otherwise("non_affected"))


    val taskDf = data(task.id)
      .where(col("batch_id") === batchId)
      .select(
        col("experiment.aliquot_id") as "aliquot_id",
        col("patient_id"),
        col("service_request_id")
      ).dropDuplicates("aliquot_id", "patient_id")

    taskDf
      .join(serviceRequestDf, Seq("service_request_id"), "left")
      .join(analysisServiceRequestWithDiseaseStatus, Seq("analysis_service_request_id", "patient_id"))
  }
}

object Variants {
  def hotspot(df: DataFrame): Column = {
    if (df.columns.contains("INFO_hotspot")) col("INFO_hotspot") as "hotspot"
    else lit(null).cast(BooleanType) as "hotspot"
  }

  @main
  def run(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = {
    Variants(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}