package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.GenomicsUtils._
import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Variants(lastBatchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_variants")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._
    val normalized_variants =
      spark.table("clin_raw.variants")
        .where(col("updatedOn") >= lastBatchId)

    val normalized_occurrences =
      spark.table("clin_raw.occurrences")

    val genomes = spark.table("clin.1000_genomes").as("1000Gp3").selectLocus($"ac", $"af", $"an")
    val topmed = spark.table("clin.topmed_bravo").selectLocus($"ac", $"af", $"an", $"hom", $"het")
    val gnomad_genomes_2_1 = spark.table("clin.gnomad_genomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af", $"an", $"hom")
    val gnomad_exomes_2_1 = spark.table("clin.gnomad_exomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af", $"an", $"hom")
    val gnomad_genomes_3_0 = spark.table("clin.gnomad_genomes_3_0").as("gnomad_genomes_3_0").selectLocus($"ac", $"af", $"an", $"hom")

    Map(
      "normalized_variants" -> normalized_variants,
      "normalized_occurrences" -> normalized_occurrences,
      "1000_genomes" -> genomes,
      "topmed" -> topmed,
      "gnomad_genomes_2_1" -> gnomad_genomes_2_1,
      "gnomad_exomes_2_1" -> gnomad_exomes_2_1,
      "gnomad_genomes_3_0" -> gnomad_genomes_3_0,
      "dbsnp" -> spark.table("clin.dbsnp"),
      "clinvar" -> spark.table("clin.clinvar"),
      "genes" -> spark.table("clin.genes")
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variants = data("normalized_variants")
    val occurrences = data("normalized_occurrences")
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end").where($"has_alt" === true)
      .as("occurrences")

    val buildDF = variantsWithFrequencies(variants, occurrences)
    val joinWithPop = joinWithPopulations(buildDF, data)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data("dbsnp"))
    val joinClinvar = joinWithClinvar(joinDbSNP, data("clinvar"))
    val joinGenes = joinWithGenes(joinClinvar, data("genes"))
    addExtDb(joinGenes)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    DeltaUtils.upsert(
      data,
      Some(destination.location),
      "clin",
      "variants",
      {
        _.repartition(1, col("chromosome")).sortWithinPartitions("start")
      },
      locusColumnNames,
      Seq("chromosome"))
    data
  }

  def variantsWithFrequencies(variants: DataFrame, occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    variants
      .withColumnRenamed("genes", "genes_symbol")
      .joinByLocus(occurrences, "inner")
      .groupBy(locus :+ col("alternate") :+ col("organization_id"): _*)
      .agg(ac, an, het, hom, participant_number,
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
        map_from_entries(collect_list(struct($"organization_id", $"lab_frequency"))) as "lab_frequencies",
      )
      .withColumn("internal_frequencies", struct($"ac", $"an", $"ac" / $"an" as "af", $"hom", $"het"))
      .select($"variant.*",
        $"donors",
        $"lab_frequencies",
        $"internal_frequencies",
        $"participant_number"
      )
      .withColumn("assembly_version", lit("GRCh38"))
      .withColumn("last_annotation_update", current_date())
      .withColumn("dna_change", concat_ws(">", $"reference", $"alternate"))
  }

  def joinWithPopulations(variants: DataFrame, data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    broadcast(variants)
      .joinAndMerge(data("1000_genomes"), "1000_genomes", "left")
      .joinAndMerge(data("topmed"), "topmed_bravo", "left")
      .joinAndMerge(data("gnomad_genomes_2_1"), "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(data("gnomad_exomes_2_1"), "exac", "left")
      .joinAndMerge(data("gnomad_genomes_3_0"), "gnomad_genomes_3_0", "left")
      .select(variants("*"), struct(col("1000_genomes"), col("topmed_bravo"), col("gnomad_genomes_2_1_1"), col("exac"), col("gnomad_genomes_3_0"), col("internal_frequencies") as "internal") as "frequencies")
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
        clinvar.selectLocus($"clinvar.name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations"),
        "clinvar",
        "left")
  }

  def joinWithGenes(variants: DataFrame, genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val genesRenamed = genes
      .withColumnRenamed("chromosome", "genes_chromosome")

    variants
      .join(genesRenamed, col("chromosome") === col("genes_chromosome") && array_contains(variants("genes_symbol"), genesRenamed("symbol")), "left")
      .drop(genesRenamed("genes_chromosome"))
      .groupByLocus()
      .agg(
        first(struct(variants("*"))) as "variant",
        collect_list(struct("genes.*")) as "genes",
        flatten(collect_set("genes.omim.omim_id")) as "omim"
      )
      .select("variant.*", "genes", "omim")
  }

  private def addExtDb(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    variants.withColumn(
      "ext_db", struct(
        $"pubmed".isNotNull.as("is_pubmed"), // ????
        $"dbsnp".isNotNull.as("is_dbsnp"),
        $"clinvar".isNotNull.as("is_clinvar"),
        exists($"genes", gene => gene("hpo").isNotNull).as("is_hpo"),
        exists($"genes", gene => gene("orphanet").isNotNull).as("is_orphanet"),
        exists($"genes", gene => gene("omim").isNotNull).as("is_omim")
      )
    )
  }
}

