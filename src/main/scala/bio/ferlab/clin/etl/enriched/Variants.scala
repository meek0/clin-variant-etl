package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Variants.{somaticTumorNormalFilter, somaticTumorOnlyFilter, DataFrameOps => ClinDataFrameOps}
import bio.ferlab.clin.etl.mainutils.OptionalChromosome
import bio.ferlab.clin.etl.utils.FrequencyUtils._
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.genomics.enriched.Variants.{DataFrameOps => LibDataFrameOps}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.{LocalDate, LocalDateTime}

case class Variants(rc: RuntimeETLContext, chromosome: Option[String]) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val snv: DatasetConf = conf.getDataset("enriched_snv")
  val snv_somatic: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val gnomad_joint_v4: DatasetConf = conf.getDataset("normalized_gnomad_joint_v4")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val cosmic: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")
  val franklin: DatasetConf = conf.getDataset("normalized_franklin")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    chromosome match {
      case Some(chr) =>
        val chromosome_condition = $"chromosome" === chr.replace("chr", "")

        Map(
          normalized_variants.id -> normalized_variants.read.where(chromosome_condition),
          snv.id -> snv.read.where(chromosome_condition),
          snv_somatic.id -> snv_somatic.read.where(chromosome_condition),
          thousand_genomes.id -> thousand_genomes.read.where(chromosome_condition),
          topmed_bravo.id -> topmed_bravo.read.where(chromosome_condition),
          gnomad_constraint.id -> gnomad_constraint.read.where(chromosome_condition),
          gnomad_joint_v4.id -> gnomad_joint_v4.read.where(chromosome_condition),
          dbsnp.id -> dbsnp.read.where(chromosome_condition),
          clinvar.id -> clinvar.read.where(chromosome_condition),
          genes.id -> genes.read,
          normalized_panels.id -> normalized_panels.read,
          cosmic.id -> cosmic.read.where(chromosome_condition),
          franklin.id -> franklin.read.where(chromosome_condition)
        )
      case None =>
        Map(
          normalized_variants.id -> normalized_variants.read,
          snv.id -> snv.read,
          snv_somatic.id -> snv_somatic.read,
          thousand_genomes.id -> thousand_genomes.read,
          topmed_bravo.id -> topmed_bravo.read,
          gnomad_constraint.id -> gnomad_constraint.read,
          gnomad_joint_v4.id -> gnomad_joint_v4.read,
          dbsnp.id -> dbsnp.read,
          clinvar.id -> clinvar.read,
          genes.id -> genes.read,
          normalized_panels.id -> normalized_panels.read,
          cosmic.id -> cosmic.read,
          franklin.id -> franklin.read
        )
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val occurrences = data(snv.id).unionByName(data(snv_somatic.id), allowMissingColumns = true)
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

    val gnomad_genomes_v4DF = data(gnomad_joint_v4.id).selectLocus($"ac_genomes" as "ac", $"af_genomes" as "af", $"an_genomes" as "an", $"hom_genomes" as "hom")
    val gnomad_exomes_v4DF = data(gnomad_joint_v4.id).selectLocus($"ac_exomes" as "ac", $"af_exomes" as "af", $"an_exomes" as "an", $"hom_exomes" as "hom")
    val gnomad_joint_v4DF = data(gnomad_joint_v4.id).selectLocus($"ac_joint" as "ac", $"af_joint" as "af", $"an_joint" as "an", $"hom_joint" as "hom")

    val joinWithDonors = variantsWithDonors(variants, occurrences)
    val joinWithCleanFreqs = cleanupSomaticTumorOnlyFreqs(joinWithDonors)
    val joinWithSomaticFreqs = joinWithSomaticFrequencies(joinWithCleanFreqs, occurrences)
    val joinWithPop = joinWithPopulations(joinWithSomaticFreqs, genomesDf, topmed_bravoDf, gnomad_genomes_v4DF, gnomad_exomes_v4DF, gnomad_joint_v4DF)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data(dbsnp.id))
    val joinClinvar = joinWithClinvar(joinDbSNP, data(clinvar.id))
    val joinGenes = joinWithGenes(joinClinvar, data(genes.id))
    val joinPanels = joinWithPanels(joinGenes, data(normalized_panels.id))

    joinPanels
      .withExomiser(occurrences)
      .withCosmic(data(cosmic.id))
      .withFranklin(data(franklin.id))
      .withGeneExternalReference
      .withClinVariantExternalReference
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("hash", sha1(col("locus"))) // if changed then modify + run https://github.com/Ferlab-Ste-Justine/clin-pipelines/blob/master/src/main/scala/bio/ferlab/clin/etl/scripts/FixFlagHashes.scala
      .withColumn(mainDestination.oid, col("updated_on"))
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100), sortColumns = Seq("start"))

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
        "variant_class", "pubmed", "created_on", "hotspot", "genes")
      .groupByLocus()
      .agg(
        first("end") as "end",
        first("name") as "name",
        first($"genes_symbol") as "genes_symbol",
        first($"hgvsg") as "hgvsg",
        first($"variant_class") as "variant_class",
        first($"pubmed") as "pubmed",
        first($"genes") as "genes",
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

  def joinWithSomaticFrequencies(variants: DataFrame, occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val occurrencesSomatic = occurrences
      .filter($"bioinfo_analysis_code".isin("TEBA", "TNEBA"))
      .selectLocus($"bioinfo_analysis_code", $"sample_id", $"filters", $"ad_alt", $"sq")
      .persist()

    val pnPerAnalysisCode = occurrencesSomatic
      .groupBy("bioinfo_analysis_code")
      .agg(
        pnSomatic
      )
    val pnSomaticTumorOnly: Long = pnPerAnalysisCode.getPnSomatic(somaticTumorOnlyFilter)
    val pnSomaticTumorNormal: Long = pnPerAnalysisCode.getPnSomatic(somaticTumorNormalFilter)

    val pcPerLocus = occurrencesSomatic
      .groupByLocus($"bioinfo_analysis_code")
      .agg(pcSomatic)

    val variantsWithSomaticFrequencies = pcPerLocus
      .withSomaticFreqColumn("freq_rqdm_tumor_only", somaticTumorOnlyFilter, pnSomaticTumorOnly)
      .withSomaticFreqColumn("freq_rqdm_tumor_normal", somaticTumorNormalFilter, pnSomaticTumorNormal)
      .groupByLocus()
      .agg(
        first("freq_rqdm_tumor_only", ignoreNulls = true) as "freq_rqdm_tumor_only",
        first("freq_rqdm_tumor_normal", ignoreNulls = true) as "freq_rqdm_tumor_normal",
      )

    variants
      .joinByLocus(variantsWithSomaticFrequencies, "left")
      // Replace nulls with empty frequency
      .withColumn("freq_rqdm_tumor_only", coalesce($"freq_rqdm_tumor_only", emptySomaticFrequency(pn = pnSomaticTumorOnly)))
      .withColumn("freq_rqdm_tumor_normal", coalesce($"freq_rqdm_tumor_normal", emptySomaticFrequency(pn = pnSomaticTumorNormal)))
  }

  def joinWithPopulations(variants: DataFrame,
                          genomesDf: DataFrame,
                          topmed_bravoDf: DataFrame,
                          gnomad_genomes_4Df: DataFrame,
                          gnomad_exomes_4Df: DataFrame,
                          gnomad_joint_4Df: DataFrame): DataFrame = {

    broadcast(variants)
      .joinAndMerge(genomesDf, "thousand_genomes", "left")
      .joinAndMerge(topmed_bravoDf, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_4Df, "gnomad_genomes_4", "left")
      .joinAndMerge(gnomad_exomes_4Df, "gnomad_exomes_4", "left")
      .joinAndMerge(gnomad_joint_4Df, "gnomad_joint_4", "left")
      .select(variants("*"),
        struct(
          col("thousand_genomes"),
          col("topmed_bravo"),
          col("gnomad_genomes_4"),
          col("gnomad_exomes_4"),
          col("gnomad_joint_4")) as "external_frequencies")
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
    val variantsWithoutGenes = variants.drop("genes")
    variants
      .select(variantsWithoutGenes("*"), explode_outer($"genes") as "gene")
      .select(variantsWithoutGenes.drop("gene")("*"), $"gene.*")
      .join(broadcast(genes), Seq("chromosome", "symbol"), "left")
      .groupByLocus()
      .agg(
        first(struct(variantsWithoutGenes("*"))) as "variant",
        collect_list(struct(genes.drop("chromosome")("*"), $"spliceai")) as "genes",
        flatten(collect_set(genes("omim.omim_id"))) as "omim"
      )
      .select("variant.*", "genes", "omim")
  }

  def joinWithPanels(variants: DataFrame, panels: DataFrame): DataFrame = {
    val variantColumns = variants.drop("chromosome", "start", "reference", "alternate").columns.map(c => first(c) as c)
    variants
      .join(broadcast(panels), array_contains(variants("genes_symbol"), panels("symbol")), "left")
      .groupByLocus()
      .agg(array_distinct(flatten(collect_list(col("panels")))) as "panels", variantColumns: _*)
      .drop("symbol", "version")
  }
}

object Variants {

  val somaticTumorOnlyFilter: Column = col("bioinfo_analysis_code") === "TEBA"
  val somaticTumorNormalFilter: Column = col("bioinfo_analysis_code") === "TNEBA"

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

      val conditionValueMap: List[(Column, String)] = List(
        $"rsnumber".isNotNull -> "DBSNP",
        $"pubmed".isNotNull -> "PubMed",
        $"clinvar".isNotNull -> "Clinvar",
        $"cmc".isNotNull -> "Cosmic",
        $"franklin_max".isNotNull -> "Franklin",
        ($"external_frequencies.gnomad_genomes_4".isNotNull or $"external_frequencies.gnomad_exomes_4".isNotNull or $"external_frequencies.gnomad_joint_4".isNotNull) -> "gnomAD"
      )
      
      withExternalReference(df, conditionValueMap)
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
  def run(rc: RuntimeETLContext, chromosome: OptionalChromosome): Unit = {
    Variants(rc, chromosome.name).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

