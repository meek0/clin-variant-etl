package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.GenomicsUtils._
import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Variants {

  def run(input: String, output: String, lastExecution: String)(implicit spark: SparkSession): Unit = {
    val inputDF =
      spark.table("clin_raw.variants")
        .where(col("updatedOn") >= lastExecution)

    val ouputDF: DataFrame = transform(inputDF)

    DeltaUtils.upsert(
      ouputDF,
      Some(output),
      "clin",
      "variants",
      {
        _.repartition(1, col("chromosome")).sortWithinPartitions("start")
      },
      locusColumnNames,
      Seq("chromosome"))

  }

  def transform(inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val buildDF = build(inputDF).persist()
    buildDF.show(1)
    inputDF.unpersist()

    val joinWithPop = joinWithPopulations(buildDF)
    val joinDbSNP = joinWithDbSNP(joinWithPop)
    val joinClinvar = joinWithClinvar(joinDbSNP)
    val joinGenes = joinWithGenes(joinClinvar)
    addExtDb(joinGenes)
  }

  def build(inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = spark.table("clin_raw.occurrences")
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end").where($"has_alt" === true)
      .as("occurrences")

    inputDF
      .withColumnRenamed("genes", "genes_symbol")
      .joinByLocus(occurrences, "inner")
      .groupBy(locus :+ col("alternate") :+ col("organization_id"): _*)
      .agg(ac, an, het, hom, participant_number,
        first(struct(inputDF("*"), $"variant_type")) as "variant",
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

  def joinWithPopulations(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val genomes = spark.table("clin.1000_genomes").as("1000Gp3").selectLocus($"ac", $"af", $"an")
    val topmed = spark.table("clin.topmed_bravo").selectLocus($"ac", $"af", $"an", $"hom", $"het")
    val gnomad_genomes_2_1 = spark.table("clin.gnomad_genomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af", $"an", $"hom")
    val gnomad_exomes_2_1 = spark.table("clin.gnomad_exomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af", $"an", $"hom")
    val gnomad_genomes_3_0 = spark.table("clin.gnomad_genomes_3_0").as("gnomad_genomes_3_0").selectLocus($"ac", $"af", $"an", $"hom")

    broadcast(variants)
      .joinAndMerge(genomes, "1000_genomes", "left")
      .joinAndMerge(topmed, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_2_1, "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(gnomad_exomes_2_1, "exac", "left")
      .joinAndMerge(gnomad_genomes_3_0, "gnomad_genomes_3_0", "left")
      .select(variants("*"), struct(col("1000_genomes"), col("topmed_bravo"), col("gnomad_genomes_2_1_1"), col("exac"), col("gnomad_genomes_3_0"), col("internal_frequencies") as "internal") as "frequencies")
      .drop("internal_frequencies")
  }

  def joinWithDbSNP(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val dbsnp = spark.table("clin.dbsnp")
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants("*"), dbsnp("name") as "dbsnp")
  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val clinvar = spark.table("clin.clinvar")
      .selectLocus($"clinvar.name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations")
    variants.joinAndMerge(clinvar, "clinvar", "left")
  }

  def joinWithGenes(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val genes = spark.table("clin.genes")
      .withColumnRenamed("chromosome", "genes_chromosome")

    variants
      .join(genes, col("chromosome") === col("genes_chromosome") && array_contains(variants("genes_symbol"), genes("symbol")), "left")
      .drop(genes("genes_chromosome"))
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

