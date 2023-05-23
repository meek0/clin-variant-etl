package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Variants._
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import bio.ferlab.datalake.spark3.utils.DeltaUtils.vacuum
import bio.ferlab.datalake.spark3.utils.FixedRepartition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.{LocalDate, LocalDateTime}

class Variants()(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val snv: DatasetConf = conf.getDataset("enriched_snv")
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
  val varsome: DatasetConf = conf.getDataset("normalized_varsome")
  val spliceai: DatasetConf = conf.getDataset("enriched_spliceai")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_variants.id -> normalized_variants.read,
      snv.id -> snv.read,
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
      varsome.id -> varsome.read,
      spliceai.id -> spliceai.read,
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val occurrences = data(snv.id)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")

    val pn_an_by_analysis: DataFrame = getPnAnPerAnalysis(occurrences)
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
    val joinWithPop = joinWithPopulations(joinWithDonors, genomesDf, topmed_bravoDf, gnomad_genomes_v2_1DF, gnomad_exomes_v2_1DF, gnomad_genomes_3_0DF, gnomad_genomes_v3DF)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data(dbsnp.id))
    val joinClinvar = joinWithClinvar(joinDbSNP, data(clinvar.id))
    val joinGenes = joinWithGenes(joinClinvar, data(genes.id))
    val joinConstraint = joinWithConstraint(joinGenes, data(gnomad_constraint.id))
    val joinPanels = joinWithPanels(joinConstraint, data(normalized_panels.id))
    val joinVarsome = joinWithVarsome(joinPanels, data(varsome.id))
    val joinSpliceAi = joinWithSpliceAi(joinVarsome, data(spliceai.id))

    joinSpliceAi
      .withGeneExternalReference
      .withVariantExternalReference
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("hash", sha1(col("locus")))
      .withColumn(mainDestination.oid, col("updated_on"))
  }

  override def defaultRepartition: DataFrame => DataFrame = FixedRepartition(56)

  override def publish()(implicit spark: SparkSession): Unit = {
    vacuum(mainDestination, 2)
  }

  private def getPnAnPerAnalysis(occurrences: DataFrame): DataFrame = {
    import occurrences.sparkSession.implicits._
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

  def mergeVariantFrequencies(variants: DataFrame, pn_an_by_analysis: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._
    val originalVariants = variants
      .select("chromosome", "start", "reference", "alternate", "end", "name", "genes_symbol", "hgvsg",
        "variant_class", "pubmed", "variant_type", "created_on")
      .groupByLocus()
      .agg(
        first("end") as "end",
        first("name") as "name",
        first($"genes_symbol") as "genes_symbol",
        first($"hgvsg") as "hgvsg",
        first($"variant_class") as "variant_class",
        first($"pubmed") as "pubmed",
        first($"variant_type") as "variant_type",
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
    val donorColumns = occurrences.drop("chromosome", "start", "end", "reference", "alternate", "exomiser_variant_score").columns.map(col)
    val donors = occurrences
      .groupByLocus()
      .agg(
        max("exomiser_variant_score") as "exomiser_variant_score",
        filter(collect_list(struct(donorColumns: _*)), c => c("has_alt")) as "donors"
      )
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
    //We first take rsnumber from variants.name, and then from dbsnp if variants.name is null
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants.drop("name")("*"), coalesce(variants("name"), dbsnp("name")) as "rsnumber")
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

  def joinWithConstraint(variants: DataFrame, constraint: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val joinCols = Seq("chromosome", "symbol")

    val gnomadStruct = constraint
      .groupBy("chromosome", "symbol")
      .agg(
        max("pLI") as "pli",
        max("oe_lof_upper") as "loeuf"
      )
      .withColumn("gnomad", struct("pli", "loeuf"))
      .select("gnomad", joinCols: _*)

    variants
      .select($"*", explode_outer($"genes") as "gene", $"gene.symbol" as "symbol") // explode_outer since genes can be null
      .join(broadcast(gnomadStruct), joinCols, "left")
      .drop("symbol") // only used for joining
      .withColumn("gene", struct($"gene.*", $"gnomad")) // add gnomad struct as nested field of gene struct
      .groupByLocus()
      .agg(
        first(struct(variants.drop("genes")("*"))) as "variant",
        collect_list("gene") as "genes" // re-create genes list for each locus, now containing gnomad struct
      )
      .select("variant.*", "genes")
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
      .agg(array_distinct(flatten(collect_list(col("panels")))) as "panels", variantColumns: _*)
      .drop("symbol", "version")
  }

  def joinWithSpliceAi(variants: DataFrame, spliceai: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

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

