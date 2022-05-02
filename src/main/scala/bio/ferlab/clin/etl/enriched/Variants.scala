package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Variants._
import bio.ferlab.clin.etl.utils.FrequencyUtils.isFilterPass
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumNames}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

class Variants()(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_3_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_1_1")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val varsome: DatasetConf = conf.getDataset("normalized_varsome")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_variants.id -> normalized_variants.read,
      normalized_snv.id -> normalized_snv.read.filter(isFilterPass),
      thousand_genomes.id -> thousand_genomes.read,
      topmed_bravo.id -> topmed_bravo.read,
      gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1.read,
      gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1.read,
      gnomad_genomes_3_0.id -> gnomad_genomes_3_0.read,
      gnomad_genomes_3_1_1.id -> gnomad_genomes_3_1_1.read,
      dbsnp.id -> dbsnp.read,
      clinvar.id -> clinvar.read,
      genes.id -> genes.read,
      normalized_panels.id -> normalized_panels.read,
      varsome.id -> varsome.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val occurrences = data(normalized_snv.id)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")
      //.drop("is_multi_allelic", "old_multi_allelic", "name", "end", "hgvsg", "variant_class", "variant_type",
      //  "genome_build", "analysis_display_name", "practitioner_role_id", "organization_id", "has_alt", "family_id",
      //  "batch_id", "last_update")

    val participantCount =
      occurrences
        .where(col("has_alt"))
        .dropDuplicates("patient_id")
        .groupBy("analysis_code", "affected_status")
        .count
        .withColumn("affected_pn", when(col("affected_status"), col("count")))
        .withColumn("non_affected_pn", when(not(col("affected_status")), col("count")))
        .groupBy("analysis_code")
        .agg(
          coalesce(first("affected_pn", true), lit(0)) as "affected_pn",
          coalesce(first("non_affected_pn", true), lit(0)) as "non_affected_pn",
          sum(col("count")) as "total_pn"
        )

    val variants = mergeVariantFrequencies(data(normalized_variants.id), participantCount)

    val genomesDf = data(`thousand_genomes`.id)
      .selectLocus($"ac".cast("long"), $"af", $"an".cast("long"))
    val topmed_bravoDf = data(topmed_bravo.id)
      .selectLocus(
        $"ac".cast("long"),
        $"af",
        $"an".cast("long"),
        $"homozygotes".cast("long") as "hom",
        $"heterozygotes".cast("long") as "het")

    val gnomad_genomes_2_1Df = data(gnomad_genomes_2_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_exomes_2_1Df = data(gnomad_exomes_2_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_genomes_3_0Df = data(gnomad_genomes_3_0.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_genomes_3_1_1Df = data(gnomad_genomes_3_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"nhomalt".cast("long") as "hom")


    //val joinWithFrequencies = variantsWithFrequencies(variants, occurrences)
    val joinWithDonors = variantsWithDonors(variants, occurrences)
    val joinWithPop = joinWithPopulations(joinWithDonors, genomesDf, topmed_bravoDf, gnomad_genomes_2_1Df, gnomad_exomes_2_1Df, gnomad_genomes_3_0Df, gnomad_genomes_3_1_1Df)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data(dbsnp.id))
    val joinClinvar = joinWithClinvar(joinDbSNP, data(clinvar.id))
    val joinGenes = joinWithGenes(joinClinvar, data(genes.id))
    val joinPanels = joinWithPanels(joinGenes, data(normalized_panels.id))
    val joinVarsome = joinWithVarsome(joinPanels, data(varsome.id))

    joinVarsome
      .withGeneExternalReference
      .withVariantExternalReference
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("hash", sha1(col("locus")))
      .withColumn("updated_on", col("created_on"))
      .withColumn(destination.oid, col("created_on"))
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(100, col("chromosome"))
    )
  }

  private def sumFrequenciesByAnalysis(column: String, group: String): Column = {
    val prefix = s"$column.$group"
    struct(
      sum(col(s"$prefix.ac")) as "ac",
      first(s"${group}_pn") * 2 as "an",
      coalesce(sum(col(s"$prefix.ac"))/sum(col(s"$prefix.an")), lit(0.0)) as "af",
      sum(col(s"$prefix.pc")) as "pc",
      first(s"${group}_pn") as "pn",
      coalesce(sum(col(s"$prefix.pc"))/sum(col(s"$prefix.pn")), lit(0.0)) as "pf",
      sum(col(s"$prefix.hom")) as "hom"
    ) as s"$group"
  }

  private def sumFrequencies(prefix: String): Column = {
    struct(
      sum(col(s"$prefix.ac")) as "ac",
      sum(col(s"${prefix}.an")) as "an",
      coalesce(sum(col(s"$prefix.ac"))/sum(col(s"$prefix.an")), lit(0.0)) as "af",
      sum(col(s"$prefix.pc")) as "pc",
      sum(col(s"${prefix}.pn")) as "pn",
      coalesce(sum(col(s"$prefix.pc"))/sum(col(s"$prefix.pn")), lit(0.0)) as "pf",
      sum(col(s"$prefix.hom")) as "hom"
    ) as s"$prefix"
  }

  def mergeVariantFrequencies(variants: DataFrame, participantCount: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variantColumns = List("end", "name", "is_multi_allelic", "old_multi_allelic", "genes_symbol", "hgvsg",
      "variant_class", "pubmed", "variant_type", "created_on").map(col)

    variants
      .withColumn( "variant", struct(variantColumns:_*))
      .withColumn("frequency_by_analysis", explode($"frequencies_by_analysis"))
      .join(participantCount, col("analysis_code") === col("frequency_by_analysis.analysis_code"))
      .groupBy(locus :+ $"frequency_by_analysis.analysis_code":_*)
      .agg(
        first($"affected_pn") as "affected_pn",
        first($"non_affected_pn") as "non_affected_pn",
        first($"total_pn") as "total_pn",
        first($"frequency_by_analysis.analysis_display_name", ignoreNulls = true) as "analysis_display_name",
        first("variant") as "variant",
        max(col("batch_id")) as "batch_id",
        sumFrequenciesByAnalysis("frequency_by_analysis", "affected"),
        sumFrequenciesByAnalysis("frequency_by_analysis", "non_affected"),
        sumFrequenciesByAnalysis("frequency_by_analysis", "total")
      )
      .groupByLocus()
      .agg(
        first("variant") as "variant",
        max(col("batch_id")) as "batch_id",
        collect_list(struct($"analysis_code", $"analysis_display_name", $"affected", $"non_affected", $"total")) as "frequencies_by_analysis",
        struct(
          sumFrequencies("affected"),
          sumFrequencies("non_affected"),
          sumFrequencies("total")
        ) as "frequency_RQDM"
      )
      .select(
        "chromosome",
        "start",
        "reference",
        "alternate",
        "variant.*",
        "frequencies_by_analysis",
        "frequency_RQDM",
        "batch_id"
      )
      .withColumn("assembly_version", lit("GRCh38"))
      .withColumn("last_annotation_update", current_date())
      .withColumn("dna_change", concat_ws(">", $"reference", $"alternate"))
  }

  def variantsWithDonors(variants: DataFrame, occurrences: DataFrame): DataFrame = {
    val donorColumns = occurrences.drop("chromosome", "start", "end", "reference", "alternate").columns.map(col)
    val donors = occurrences
      .groupByLocus()
      .agg(filter(collect_list(struct(donorColumns:_*)), c => c("has_alt")) as "donors")
    variants
      .joinByLocus(donors, "inner")
  }

  def joinWithPopulations(variants: DataFrame,
                          genomesDf: DataFrame,
                          topmed_bravoDf: DataFrame,
                          gnomad_genomes_2_1Df: DataFrame,
                          gnomad_exomes_2_1Df: DataFrame,
                          gnomad_genomes_3_0Df: DataFrame,
                          gnomad_genomes_3_1_1Df: DataFrame)(implicit spark: SparkSession): DataFrame = {

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

  def joinWithDbSNP(variants: DataFrame, dbsnp: DataFrame)(implicit spark: SparkSession): DataFrame = {
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants.drop("name")("*"), dbsnp("name") as "rsnumber")
  }

  def joinWithClinvar(variants: DataFrame, clinvar: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    variants
      .joinAndMerge(
        clinvar.selectLocus($"name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations"),
        "clinvar",
        "left")
  }

  def joinWithGenes(variants: DataFrame, genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
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

  def joinWithVarsome(variants: DataFrame, varsome: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df = varsome.select($"chromosome", $"start", $"reference", $"alternate", $"variant_id",
      $"publications.publications" as "publications",
      size($"publications.publications") > 0 as "has_publication",
      struct(
        $"acmg_annotation.verdict.ACMG_rules" as "verdict",
        $"acmg_annotation.classifications" as "classifications",
        $"acmg_annotation.transcript" as "transcript",
        $"acmg_annotation.transcript_reason" as "transcript_reason",
        $"acmg_annotation.gene_symbol" as "gene_symbol",
        $"acmg_annotation.coding_impact" as "coding_impact"
      ) as "acmg"
    )
    variants.joinAndMerge(df, "varsome", "left")

  }

  def joinWithPanels(variants: DataFrame, panels: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val variantColumns = variants.drop("chromosome", "start", "reference", "alternate").columns.map(c => first(c) as c)
    variants
      .join(panels, array_contains(variants("genes_symbol"), panels("symbol")), "left")
      .groupByLocus()
      .agg(array_distinct(flatten(collect_list(col("panels")))) as "panels", variantColumns:_*)
      .drop("symbol", "version")
  }

}

object Variants {
  implicit class DataFrameOps(df: DataFrame) {
    def withGeneExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "gene_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        exists($"genes", gene => gene("orphanet").isNotNull and size(gene("orphanet")) > 0) -> "Orphanet",
        exists($"genes", gene => gene("omim").isNotNull and size(gene("omim")) > 0) -> "OMIM"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when(exists($"genes", gene => gene("hpo").isNotNull and size(gene("hpo")) > 0), array(lit("HPO"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
    }

    def withVariantExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "variant_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        $"clinvar".isNotNull -> "Clinvar",
        $"pubmed".isNotNull -> "Pubmed"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when($"rsnumber".isNotNull, array(lit("DBSNP"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
    }

  }
}

