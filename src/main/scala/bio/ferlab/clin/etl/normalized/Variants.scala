package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.normalized.Variants._
import bio.ferlab.clin.etl.utils.FrequencyUtils
import bio.ferlab.clin.etl.utils.FrequencyUtils.{emptyFrequencies, emptyFrequency, emptyFrequencyRQDM}
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.{GenomicOperations, vcf}
import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.slf4j.Logger

import java.time.LocalDateTime

case class Variants(rc: RuntimeETLContext, batchId: String) extends SimpleSingleETL(rc) {

  import spark.implicits._

  implicit val logger: Logger = log

  override val mainDestination: DatasetConf = conf.getDataset("normalized_variants")
  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val enriched_spliceai_snv: DatasetConf = conf.getDataset("enriched_spliceai_snv")
  val enriched_spliceai_indel: DatasetConf = conf.getDataset("enriched_spliceai_indel")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), None, optional = true, split = true),
      enriched_clinical.id -> enriched_clinical.read,
      enriched_spliceai_snv.id -> enriched_spliceai_snv.read,
      enriched_spliceai_indel.id -> enriched_spliceai_indel.read
    )
  }

  private def getVCF(data: Map[String, DataFrame]): (DataFrame, String, String, Boolean) = {

    val vcf = data(raw_variant_calling.id)

    if (!vcf.isEmpty) {
      val filteredVcf = vcf.where(col("contigName").isin(validContigNames: _*))
      val genotypesIndex = vcf.schema.fieldIndex("genotypes")
      val genotypesFields = vcf.schema(genotypesIndex).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames

      if (genotypesFields.contains("conditionalQuality")) {
        // Germline VCF
        (filteredVcf, "genotype.conditionalQuality", "gq", true)
      } else if (genotypesFields.contains("SQ")) {
        // Somatic VCF
        (filteredVcf, "genotype.SQ", "sq", false)
      } else {
        throw new Exception("No valid raw VCF available")
      }
    } else {
      throw new Exception("No valid raw VCF available")
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {

    val (inputVCF, srcScoreColumn, dstScoreColumn, computeFrequencies) = getVCF(data)

    val clinicalInfos = data(enriched_clinical.id)
      .where($"batch_id" === batchId)
      .select(
        "analysis_code",
        "analysis_display_name",
        "affected_status",
        "aliquot_id",
        "bioinfo_analysis_code",
        "analysis_id"
      )
      .withColumn("affected_status_str", when(col("affected_status"), lit("affected")).otherwise("non_affected"))

    // Data per unique variant positions (no genotype / sample specific information)
    val uniqueVariants = getUniqueVariants(inputVCF, clinicalInfos)

    val sampleVariants = getSampleVariants(inputVCF, clinicalInfos, srcScoreColumn, dstScoreColumn)

    val res = getVariantsWithFrequencies(sampleVariants, computeFrequencies)
      .join(uniqueVariants, locusColumnNames :+ "analysis_id" :+ "bioinfo_analysis_code", "right")
      .partitionForLocusJoins()
      .cacheRDD() // Caching here helps Spark build the execution plan faster and reduces executor idle time between stages.
      .withSpliceAi(snv = data(enriched_spliceai_snv.id), indel = data(enriched_spliceai_indel.id))
      .withColumn("frequencies_by_analysis", coalesce(col("frequencies_by_analysis"), array(emptyFrequencies)))
      .withColumn("frequency_RQDM", coalesce(col("frequency_RQDM"), emptyFrequencyRQDM))
      .withColumn("batch_id", lit(batchId))
      .withColumn("created_on", current_timestamp())

    res
  }

  def getUniqueVariants(vcf: DataFrame, clinicalInfos: DataFrame): DataFrame = {
    vcf
      .withColumn("annotation", firstCsq)
      .withColumn("alleleDepths", functions.transform(col("genotypes.alleleDepths"), c => c(1)))
      .filter(size(filter(col("alleleDepths"), ad => ad >= 3)) > 0)
      .filter(alternate =!= "*")
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
        hotspot(vcf),
        explode(col("genotypes.sampleId")) as "aliquot_id",
      )
      .join(broadcast(clinicalInfos.select("aliquot_id", "analysis_id", "bioinfo_analysis_code")), Seq("aliquot_id"), "inner")
      .drop("aliquot_id")
      .dropDuplicates("analysis_id", "bioinfo_analysis_code", "chromosome", "start", "reference", "alternate")
  }

  def getSampleVariants(vcf: DataFrame, clinicalInfos: DataFrame, srcScoreColumn: String, dstScoreColumn: String): DataFrame = {
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
      .join(broadcast(clinicalInfos), Seq("aliquot_id"), "inner")
  }

  def getVariantsEmptyFrequencies(variants: DataFrame): DataFrame = {
    variants
      .selectLocus(col("analysis_id"), col("bioinfo_analysis_code"))
      .dropDuplicates("analysis_id", "bioinfo_analysis_code", "chromosome", "start", "reference", "alternate")
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
      .groupBy(locus :+ col("analysis_id") :+ col("bioinfo_analysis_code") :+ col("analysis_code") :+ col("affected_status_str"): _*)
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
      .groupBy(locus :+ col("analysis_id") :+ col("bioinfo_analysis_code") :+ col("analysis_code"): _*)
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
      .groupBy(locus :+ col("analysis_id") :+ col("bioinfo_analysis_code"): _*)
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
      .select(locus :+ col("analysis_id") :+ col("bioinfo_analysis_code") :+ $"frequencies_by_analysis" :+ $"frequency_RQDM": _*)
  }
}

object Variants {

  implicit class DataFrameOps(df: DataFrame) {

    // to optimize locus-based joins and aggregations
    def partitionForLocusJoins(): DataFrame = {
      RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100), sortColumns = Seq("start"))(df)
    }

    def withSpliceAi(snv: DataFrame, indel: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      def joinAndMergeIntoGenes(variants: DataFrame, spliceai: DataFrame): DataFrame = {
        if (!variants.isEmpty) {
          variants
            .select($"*", explode_outer($"genes_symbol") as "symbol") // explode_outer since genes_symbol can be empty
            .join(spliceai, locusColumnNames :+ "symbol", "left")
            .select(
              variants("*"),
              struct(
                $"symbol",
                $"spliceai"
              ) as "gene" // add spliceai struct as nested field of gene struct
            )
            .groupByLocus($"analysis_id", $"bioinfo_analysis_code")
            .agg(
              first(struct(variants.drop("genes")("*"))) as "variant",
              collect_list("gene") as "genes" // Create genes list for each locus
            )
            .select("variant.*", "genes")
        } else variants
      }

      val spliceAiSnvPrepared = snv
        .selectLocus($"symbol", $"max_score" as "spliceai")

      val spliceAiIndelPrepared = indel
        .selectLocus($"symbol", $"max_score" as "spliceai")

      val snvVariants = df
        .where($"variant_class" === "SNV")

      val otherVariants = df
        .where($"variant_class" =!= "SNV")

      val snvVariantsWithSpliceAi = joinAndMergeIntoGenes(snvVariants, spliceAiSnvPrepared)
      val otherVariantsWithSpliceAi = joinAndMergeIntoGenes(otherVariants, spliceAiIndelPrepared)

      snvVariantsWithSpliceAi.unionByName(otherVariantsWithSpliceAi, allowMissingColumns = true)
    }
  }

  def hotspot(df: DataFrame): Column = {
    if (df.columns.contains("INFO_hotspot")) col("INFO_hotspot") as "hotspot"
    else lit(null).cast(BooleanType) as "hotspot"
  }

  @main
  def run(rc: RuntimeETLContext, batch: Batch): Unit = {
    Variants(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}