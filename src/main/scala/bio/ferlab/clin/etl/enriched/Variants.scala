package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.VcfUtils.{ac, an, het, hom, participant_number}
import bio.ferlab.clin.etl.vcf.Occurrences
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Variants(lastBatchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_occurrences")
  val `1000_genomes`: DatasetConf = conf.getDataset("1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("gnomad_genomes_3_0")
  val gnomad_genomes_3_1_1: DatasetConf = conf.getDataset("gnomad_genomes_3_1_1")
  val dbsnp: DatasetConf = conf.getDataset("dbsnp")
  val clinvar: DatasetConf = conf.getDataset("clinvar")
  val genes: DatasetConf = conf.getDataset("genes")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      normalized_variants.id -> normalized_variants.read.where(col("updatedOn") >= lastBatchId),
      normalized_occurrences.id -> normalized_occurrences.read,
      `1000_genomes`.id -> `1000_genomes`.read,
      topmed_bravo.id -> topmed_bravo.read,
      gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1.read,
      gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1.read,
      gnomad_genomes_3_0.id -> gnomad_genomes_3_0.read,
      gnomad_genomes_3_1_1.id -> gnomad_genomes_3_1_1.read,
      dbsnp.id -> dbsnp.read,
      clinvar.id -> clinvar.read,
      genes.id -> genes.read
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variants = data(normalized_variants.id)
    val occurrences = data(normalized_occurrences.id)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")
      .where($"has_alt" === true)
      .as("occurrences")

    val genomesDf = data(`1000_genomes`.id)
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


    val joinWithTransmissions = variantsWithAggregate("transmission", variants, occurrences)
    val joinWithParentalOrigin = variantsWithAggregate("parental_origin", joinWithTransmissions, occurrences)
    val joinWithFrequencies = variantsWithFrequencies(joinWithParentalOrigin, occurrences)
    val joinWithPop = joinWithPopulations(joinWithFrequencies, genomesDf, topmed_bravoDf, gnomad_genomes_2_1Df, gnomad_exomes_2_1Df, gnomad_genomes_3_0Df, gnomad_genomes_3_1_1Df)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data("dbsnp"))
    val joinClinvar = joinWithClinvar(joinDbSNP, data("clinvar"))
    val joinGenes = joinWithGenes(joinClinvar, data("genes"))
    addExtDb(joinGenes)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }

  def variantsWithAggregate(aggregate: String, variants: DataFrame, occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val aggregateVariantLevel =
      occurrences
        .groupBy(locus :+ col(aggregate): _*)
        .agg(count(aggregate) as s"${aggregate}_count")
        .groupBy(locus : _*)
        .agg(
          map_from_entries(filter(collect_list(struct(col(aggregate), col(s"${aggregate}_count"))), c => c(aggregate).isNotNull)) as s"${aggregate}s",
        )

    val aggregateByLab =
      occurrences
        .groupBy(locus :+ col(aggregate) :+ col("organization_id"): _*)
        .agg(count(aggregate) as s"${aggregate}_count_by_lab")
        .groupBy(locus :+ col("organization_id"): _*)
        .agg(
          map_from_entries(filter(collect_list(struct(col(aggregate), col(s"${aggregate}_count_by_lab"))), c => c(aggregate).isNotNull)) as s"${aggregate}s_by_lab",
        )
        .groupBy(locus : _*)
        .agg(map_from_entries(collect_list(struct(col("organization_id"), col(s"${aggregate}s_by_lab")))) as s"${aggregate}s_by_lab")

    variants
      .joinByLocus(aggregateVariantLevel, "left")
      .joinByLocus(aggregateByLab, "left")
  }

  def variantsWithFrequencies(variants: DataFrame, occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    variants
      .joinByLocus(occurrences, "inner")
      .groupBy(locus :+ col("organization_id"): _*)
      .agg(
        ac,
        an,
        het,
        hom,
        participant_number,
        first(struct(variants("*"), $"variant_type")) as "variant",
        collect_list(struct("occurrences.*")) as "donors")
      .withColumn("lab_frequency", struct($"ac", $"an", $"ac" / $"an" as "af", $"hom", $"het"))
      .groupByLocus()
      .agg(
        first(col("variant")) as "variant",
        flatten(collect_list(col("donors"))) as "donors",
        sum(col("ac")) as "ac",
        sum(col("an")) as "an",
        sum(col("het")) as "het",
        sum(col("hom")) as "hom",
        sum(col("participant_number")) as "participant_number",
        map_from_entries(collect_list(struct($"organization_id", $"lab_frequency"))) as "frequencies_by_lab",
      )
      .withColumn("internal_frequencies", struct($"ac", $"an", $"ac" / $"an" as "af", $"hom", $"het"))
      .select($"variant.*",
        $"donors",
        $"frequencies_by_lab",
        $"internal_frequencies",
        $"participant_number"
      )
      .withColumn("assembly_version", lit("GRCh38"))
      .withColumn("last_annotation_update", current_date())
      .withColumn("dna_change", concat_ws(">", $"reference", $"alternate"))
  }

  def joinWithPopulations(variants: DataFrame,
                          genomesDf: DataFrame,
                          topmed_bravoDf: DataFrame,
                          gnomad_genomes_2_1Df: DataFrame,
                          gnomad_exomes_2_1Df: DataFrame,
                          gnomad_genomes_3_0Df: DataFrame,
                          gnomad_genomes_3_1_1Df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    broadcast(variants)
      .joinAndMerge(genomesDf, "1000_genomes", "left")
      .joinAndMerge(topmed_bravoDf, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_2_1Df, "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(gnomad_exomes_2_1Df, "gnomad_exomes_2_1_1", "left")
      .joinAndMerge(gnomad_genomes_3_0Df, "gnomad_genomes_3_0", "left")
      .joinAndMerge(gnomad_genomes_3_1_1Df, "gnomad_genomes_3_1_1", "left")
      .select(variants("*"),
        struct(
          col("1000_genomes"),
          col("topmed_bravo"),
          col("gnomad_genomes_2_1_1"),
          col("gnomad_exomes_2_1_1"),
          col("gnomad_genomes_3_0"),
          col("gnomad_genomes_3_1_1"),
          col("internal_frequencies") as "internal") as "frequencies")
      .drop("internal_frequencies")
  }

  def joinWithDbSNP(variants: DataFrame, dbsnp: DataFrame)(implicit spark: SparkSession): DataFrame = {
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants("*"), dbsnp("name") as "dbsnp")
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

  private def addExtDb(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    variants
      .withColumn(
        "ext_db", struct(
          $"pubmed".isNotNull.as("is_pubmed"),
          $"dbsnp".isNotNull.as("is_dbsnp"),
          $"clinvar".isNotNull.as("is_clinvar"),
          exists($"genes", gene => gene("hpo").isNotNull).as("is_hpo"),
          exists($"genes", gene => gene("orphanet").isNotNull).as("is_orphanet"),
          exists($"genes", gene => gene("omim").isNotNull).as("is_omim")
      )
    )
  }
}

