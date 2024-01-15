package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Variants.{DataFrameOps => ClinDataFrameOps}
import bio.ferlab.clin.etl.utils.FrequencyUtils.emptyFrequencyRQDM
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext, FixedRepartition}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.genomics.enriched.Variants.{DataFrameOps => LibDataFrameOps}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import bio.ferlab.datalake.spark3.utils.DeltaUtils.vacuum
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.time.{LocalDate, LocalDateTime}

case class Variants(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val snv: DatasetConf = conf.getDataset("enriched_snv")
  val snv_somatic_tumor_only: DatasetConf = conf.getDataset("enriched_snv_somatic_tumor_only")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val gnomad_genomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")
  val gnomad_exomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_v2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_v3: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val spliceai: DatasetConf = conf.getDataset("enriched_spliceai")
  val cosmic: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")
  val franklin: DatasetConf = conf.getDataset("normalized_franklin")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_variants.id -> normalized_variants.read,
      snv.id -> snv.read,
      snv_somatic_tumor_only.id -> snv_somatic_tumor_only.read,
      thousand_genomes.id -> thousand_genomes.read,
      topmed_bravo.id -> topmed_bravo.read,
      gnomad_constraint.id -> gnomad_constraint.read,
      gnomad_genomes_v2_1_1.id -> gnomad_genomes_v2_1_1.read,
      gnomad_exomes_v2_1_1.id -> gnomad_exomes_v2_1_1.read,
      gnomad_genomes_3_0.id -> gnomad_genomes_3_0.read,
      gnomad_genomes_v3.id -> gnomad_genomes_v3.read,
      dbsnp.id -> dbsnp.read,
      clinvar.id -> clinvar.read,
      genes.id -> genes.read,
      normalized_panels.id -> normalized_panels.read,
      spliceai.id -> spliceai.read,
      cosmic.id -> cosmic.read,
      franklin.id -> franklin.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val occurrences = data(snv.id).unionByName(data(snv_somatic_tumor_only.id), allowMissingColumns = true)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")

    val pnOccurrences = data(snv.id)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")

    val pn_an_by_analysis: DataFrame = getPnAnPerAnalysis(pnOccurrences)
    val variants = mergeVariantFrequencies(data(normalized_variants.id), pn_an_by_analysis)

    val genomesDf = data(`thousand_genomes`.id)
      .selectLocus($"ac".cast("long"), $"af", $"an".cast("long"))
    val topmed_bravoDf = data(topmed_bravo.id)
      .selectLocus(
        $"ac".cast("long"),
        $"af",
        $"an".cast("long"),
        $"homozygotes".cast("long") as "hom",
        $"heterozygotes".cast("long") as "het")

    val gnomad_genomes_v2_1DF = data(gnomad_genomes_v2_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_exomes_v2_1DF = data(gnomad_exomes_v2_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_genomes_3_0DF = data(gnomad_genomes_3_0.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_genomes_v3DF = data(gnomad_genomes_v3.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"nhomalt".cast("long") as "hom")

    val joinWithDonors = variantsWithDonors(variants, occurrences)
    val joinWithCleanFreqs = cleanupSomaticTumorOnlyFreqs(joinWithDonors)
    val joinWithPop = joinWithPopulations(joinWithCleanFreqs, genomesDf, topmed_bravoDf, gnomad_genomes_v2_1DF, gnomad_exomes_v2_1DF, gnomad_genomes_3_0DF, gnomad_genomes_v3DF)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data(dbsnp.id))
    val joinClinvar = joinWithClinvar(joinDbSNP, data(clinvar.id))
    val joinGenes = joinWithGenes(joinClinvar, data(genes.id))
    val joinPanels = joinWithPanels(joinGenes, data(normalized_panels.id))
    val joinSpliceAi = joinWithSpliceAi(joinPanels, data(spliceai.id))

    joinSpliceAi
      .withExomiser(occurrences)
      .withCosmic(data(cosmic.id))
      .withFranklin(data(franklin.id))
      .withGeneExternalReference
      .withClinVariantExternalReference
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("hash", sha1(col("locus")))
      .withColumn(mainDestination.oid, col("updated_on"))
  }

  override def defaultRepartition: DataFrame => DataFrame = FixedRepartition(56)

  override def publish(): Unit = {
    vacuum(mainDestination, 2)
  }

  private def getPnAnPerAnalysis(occurrences: DataFrame): DataFrame = {
    val byAnalysis = occurrences
      .select($"patient_id", $"affected_status", $"analysis_code")
      .distinct
      .groupBy("analysis_code", "affected_status")
      .agg(count(col("patient_id")) as "pn")
      .groupBy("analysis_code")
      .agg(
        struct(
          sum(when($"affected_status", $"pn").otherwise(0)) as "affected_pn",
          sum(when(not($"affected_status"), $"pn").otherwise(0)) as "non_affected_pn",
          sum($"pn") as "total_pn",
          lit(2) * sum(when($"affected_status", $"pn").otherwise(0)) as "affected_an",
          lit(2) * sum(when(not($"affected_status"), $"pn").otherwise(0)) as "non_affected_an",
          lit(2) * sum($"pn") as "total_an"
        ) as "pn_an_by_analysis"
      )

    val total = byAnalysis.select(
      struct(
        sum($"pn_an_by_analysis.affected_pn") as "affected_pn",
        sum($"pn_an_by_analysis.non_affected_pn") as "non_affected_pn",
        sum($"pn_an_by_analysis.total_pn") as "total_pn",
        sum($"pn_an_by_analysis.affected_an") as "affected_an",
        sum($"pn_an_by_analysis.non_affected_an") as "non_affected_an",
        sum($"pn_an_by_analysis.total_an") as "total_an",
      ) as "pn_an_total"
    )
    byAnalysis.join(total)
  }

  private def sumFrequencies(prefix: String, as: String, byAnalysis: Boolean = true): Column = {
    val mode = if (byAnalysis) "by_analysis" else "total"
    val total_an = first(s"pn_an_$mode.${as}_an")
    val total_pn = first(s"pn_an_$mode.${as}_pn")
    struct(
      sum(col(s"$prefix.ac")) as "ac",
      total_an as "an",
      coalesce(sum(col(s"$prefix.ac")) / total_an, lit(0.0)) as "af",
      sum(col(s"$prefix.pc")) as "pc",
      total_pn as "pn",
      coalesce(sum(col(s"$prefix.pc")) / total_pn, lit(0.0)) as "pf",
      sum(col(s"$prefix.hom")) as "hom"
    ) as as
  }

  def mergeVariantFrequencies(variants: DataFrame, pn_an_by_analysis: DataFrame): DataFrame = {

    val originalVariants = variants
      .select("chromosome", "start", "reference", "alternate", "end", "name", "genes_symbol", "hgvsg",
        "variant_class", "pubmed", "created_on", "hotspot")
      .groupByLocus()
      .agg(
        first("end") as "end",
        first("name") as "name",
        first($"genes_symbol") as "genes_symbol",
        first($"hgvsg") as "hgvsg",
        first($"variant_class") as "variant_class",
        first($"pubmed") as "pubmed",
        max($"hotspot") as "hotspot",
        max($"created_on") as "updated_on",
        min($"created_on") as "created_on"
      )

    val byAnalysis = variants
      .withColumn("frequency_by_analysis", explode_outer($"frequencies_by_analysis"))
      .join(pn_an_by_analysis, col("frequency_by_analysis.analysis_code") === col("analysis_code"))
      .groupBy(locus :+ $"frequency_by_analysis.analysis_code": _*)
      .agg(
        first($"frequency_by_analysis.analysis_display_name", ignoreNulls = true) as "analysis_display_name",
        first($"pn_an_total") as "pn_an_total",
        sumFrequencies("frequency_by_analysis.affected", "affected"),
        sumFrequencies("frequency_by_analysis.non_affected", "non_affected"),
        sumFrequencies("frequency_by_analysis.total", "total")
      )

    val variantsWithFrequencies = byAnalysis
      .groupByLocus()
      .agg(
        collect_list(struct($"analysis_code", $"analysis_display_name", $"affected", $"non_affected", $"total")) as "frequencies_by_analysis",
        struct(
          sumFrequencies("affected", "affected", byAnalysis = false),
          sumFrequencies("non_affected", "non_affected", byAnalysis = false),
          sumFrequencies("total", "total", byAnalysis = false)
        ) as "frequency_RQDM"
      )
      .select(
        "chromosome",
        "start",
        "reference",
        "alternate",
        "frequencies_by_analysis",
        "frequency_RQDM"
      )
    variantsWithFrequencies.joinByLocus(originalVariants, "right")
      .withColumn("assembly_version", lit("GRCh38"))
      .withColumn("last_annotation_update", lit(LocalDate.now()))
      .withColumn("dna_change", concat_ws(">", $"reference", $"alternate"))
  }

  def variantsWithDonors(variants: DataFrame, occurrences: DataFrame): DataFrame = {
    val donorColumns = occurrences.drop("chromosome", "start", "end", "reference", "alternate").columns.map(col)
    val donors = occurrences
      .groupByLocus()
      .agg(
        collect_set($"variant_type") as "variant_type",
        filter(collect_list(struct(donorColumns: _*)), c => c("has_alt")) as "donors"
      )
    variants
      .joinByLocus(donors, "inner")
  }

  def cleanupSomaticTumorOnlyFreqs(variants: DataFrame) = {
    val isSomaticTumorOnlyCondition = array_contains(col("variant_type"), "somatic") && size(col("variant_type")) === 1

    variants
      .withColumn("frequencies_by_analysis", when(isSomaticTumorOnlyCondition, lit(array())).otherwise(coalesce(col("frequencies_by_analysis"), lit(array()))))
      .withColumn("frequency_RQDM", when(isSomaticTumorOnlyCondition, lit(emptyFrequencyRQDM)).otherwise(coalesce(col("frequency_RQDM"), emptyFrequencyRQDM)))
  }

  def joinWithPopulations(variants: DataFrame,
                          genomesDf: DataFrame,
                          topmed_bravoDf: DataFrame,
                          gnomad_genomes_2_1Df: DataFrame,
                          gnomad_exomes_2_1Df: DataFrame,
                          gnomad_genomes_3_0Df: DataFrame,
                          gnomad_genomes_3_1_1Df: DataFrame): DataFrame = {

    broadcast(variants)
      .joinAndMerge(genomesDf, "thousand_genomes", "left")
      .joinAndMerge(topmed_bravoDf, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_2_1Df, "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(gnomad_exomes_2_1Df, "gnomad_exomes_2_1_1", "left")
      .joinAndMerge(gnomad_genomes_3_0Df, "gnomad_genomes_3_0", "left")
      .joinAndMerge(gnomad_genomes_3_1_1Df, "gnomad_genomes_3_1_1", "left")
      .select(variants("*"),
        struct(
          col("thousand_genomes"),
          col("topmed_bravo"),
          col("gnomad_genomes_2_1_1"),
          col("gnomad_exomes_2_1_1"),
          col("gnomad_genomes_3_0"),
          col("gnomad_genomes_3_1_1")) as "external_frequencies")
  }

  def joinWithDbSNP(variants: DataFrame, dbsnp: DataFrame): DataFrame = {
    //We first take rsnumber from variants.name, and then from dbsnp if variants.name is null
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants.drop("name")("*"), coalesce(variants("name"), dbsnp("name")) as "rsnumber")
  }

  def joinWithClinvar(variants: DataFrame, clinvar: DataFrame): DataFrame = {
    variants
      .joinAndMerge(
        clinvar.selectLocus($"name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations"),
        "clinvar",
        "left")
  }

  def joinWithGenes(variants: DataFrame, genes: DataFrame): DataFrame = {
    variants
      .join(genes, variants("chromosome") === genes("chromosome") && array_contains(variants("genes_symbol"), genes("symbol")), "left")
      .drop(genes("chromosome"))
      .groupByLocus()
      .agg(
        first(struct(variants("*"))) as "variant",
        collect_list(struct(genes.drop("chromosome")("*"))) as "genes",
        flatten(collect_set(genes("omim.omim_id"))) as "omim"
      )
      .select("variant.*", "genes", "omim")
  }

  def joinWithPanels(variants: DataFrame, panels: DataFrame): DataFrame = {
    val variantColumns = variants.drop("chromosome", "start", "reference", "alternate").columns.map(c => first(c) as c)
    variants
      .join(panels, array_contains(variants("genes_symbol"), panels("symbol")), "left")
      .groupByLocus()
      .agg(array_distinct(flatten(collect_list(col("panels")))) as "panels", variantColumns: _*)
      .drop("symbol", "version")
  }

  def joinWithSpliceAi(variants: DataFrame, spliceai: DataFrame): DataFrame = {
    val scores = spliceai.selectLocus($"symbol", $"max_score" as "spliceai")
      .withColumn("type", when($"spliceai.ds" === 0, null).otherwise($"spliceai.type"))
      .withColumn("spliceai", struct($"spliceai.ds" as "ds", $"type"))
      .drop("type")

    variants
      .select($"*", explode_outer($"genes") as "gene", $"gene.symbol" as "symbol") // explode_outer since genes can be null
      .join(scores, locusColumnNames :+ "symbol", "left")
      .drop("symbol") // only used for joining
      .withColumn("gene", struct($"gene.*", $"spliceai")) // add spliceai struct as nested field of gene struct
      .groupByLocus()
      .agg(
        first(struct(variants.drop("genes")("*"))) as "variant",
        collect_list("gene") as "genes" // re-create genes list for each locus, now containing spliceai struct
      )
      .select("variant.*", "genes")
  }
}

object Variants {

  implicit class DataFrameOps(df: DataFrame) {
    def withFranklin(franklin: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val franklinPrepared = franklin
        .filter($"aliquot_id".isNotNull) // Remove analyses in trio
        .groupByLocus()
        .agg(
          // Always the same link, ACMG classification and evidence for all occurrences of a variant
          firstAs("acmg_classification", ignoreNulls = true),
          firstAs("acmg_evidence", ignoreNulls = true),
          firstAs("link", ignoreNulls = true),
          max("score") as "combined_score"
        )

      df.joinAndMerge(franklinPrepared, "franklin_max", "left")
    }

    def withClinVariantExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "variant_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        $"rsnumber".isNotNull -> "DBSNP",
        $"pubmed".isNotNull -> "PubMed",
        $"clinvar".isNotNull -> "Clinvar",
        $"cmc".isNotNull -> "Cosmic",
        $"franklin_max".isNotNull -> "Franklin"
      )

      conditionValueMap
        .tail
        .foldLeft(
          df.withColumn(outputColumn, when(conditionValueMap.head._1, array(lit(conditionValueMap.head._2))).otherwise(array()))
        ) { case (currDf, (cond, value)) =>
          currDf.withColumn(outputColumn, when(cond, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
        }
    }

    def withExomiser(donors: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val maxExomiser = donors
        .where($"exomiser".isNotNull)
        .selectLocus($"exomiser".dropFields("rank") as "exomiser") // rank is not needed in exomiser_max struct
        .groupByLocus()
        .agg(
          max_by($"exomiser", $"exomiser.gene_combined_score") as "exomiser_max"
        )

      df.joinByLocus(maxExomiser, "left")
    }
  }

  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    Variants(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

