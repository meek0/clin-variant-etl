package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.ByLocus._
import bio.ferlab.clin.etl.columns.{ac, formatted_consequences, het, hom}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PrepareIndex extends App {

  val Array(output, batchId) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Prepare Index").getOrCreate()

  run(output, batchId)

  def run(output: String, batchId: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val joinVariants: String => DataFrame = (buildNewVariants _)
      .andThen(joinWithPopulations)
      .andThen(joinWithClinvar)
      .andThen(joinWithDbSNP)
      .andThen(joinWithGenes)
      .andThen(addExtDb)

    val finalDf = joinVariants(batchId)
    finalDf
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/extract")

    //    val updatedVariants = spark.table("variants").where($"last_batch_id" === batchId)
    //      .join(occurrences, newVariants("chromosome") === occurrences("chromosome") && newVariants("start") === occurrences("start") && newVariants("reference") === occurrences("reference") && newVariants("alternate") === occurrences("alternate"))

    finalDf
  }

  private def buildNewVariants(batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val newVariants = spark.table("variants")
      .where($"batch_id" === batchId)
      .withColumnRenamed("genes", "genes_symbol")
      .withColumn("assembly_version", lit("GRCh38"))
      .withColumn("last_annotation_update", current_date())
      .as("variants")

    val consequences = buildConsequences(spark).as("consequences")

    val joinWithConsequences = newVariants
      .joinByLocus(consequences, "inner")
      .groupByLocus()
      .agg(
        first(struct("variants.*")) as "variant",
        collect_list(struct("consequences.*")) as "consequences",
        max("impact_score") as "impact_score")
      .select($"variant.*", $"consequences", $"impact_score")

    val occurrences = spark.table("occurrences")
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end").where($"has_alt" === true)
      .as("occurrences")

    val nbParticipantsWithOccurrences: Long = occurrences.select(countDistinct($"patient_id")).as[Long].collect().head
    val allelesNumber = nbParticipantsWithOccurrences * 2
    joinWithConsequences
      .joinByLocus(occurrences, "inner")
      .groupByLocus()
      .agg(
        first(struct(joinWithConsequences("*"))) as "variant",
        collect_list(struct("occurrences.*")) as "donors",
        ac,
        lit(allelesNumber) as "an",
        het,
        hom
      )
      .withColumn("internal_frequencies", struct($"ac", $"an", $"ac" / $"an" as "af", $"hom", $"het"))
      .select($"variant.*",
        $"donors",
        $"internal_frequencies"
      )
      .withColumn("dna_change", concat_ws(">", $"reference", $"alternate"))
  }

  private def buildConsequences(implicit spark: SparkSession): DataFrame = {

    val csq = spark.table("consequences")
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_transcript_id", "ensembl_regulatory_id")
      .withColumn("consequence", formatted_consequences)
      .as("consequences")

    joinWithDBNSFP(csq)
  }

  def joinWithGenes(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val genes = spark.table("genes")
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

  def joinWithPopulations(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val genomes = spark.table("1000_genomes").as("1000Gp3").selectLocus($"ac", $"af", $"an")
    val topmed = spark.table("topmed_bravo").selectLocus($"ac", $"af", $"an", $"hom", $"het")
    val gnomad_genomes_2_1 = spark.table("gnomad_genomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af", $"an", $"hom")
    val gnomad_exomes_2_1 = spark.table("gnomad_exomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af", $"an", $"hom")
    val gnomad_genomes_3_0 = spark.table("gnomad_genomes_3_0").as("gnomad_genomes_3_0").selectLocus($"ac", $"af", $"an", $"hom")

    variants.joinAndMerge(genomes, "1000_genomes", "left")
      .joinAndMerge(topmed, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_2_1, "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(gnomad_exomes_2_1, "exac", "left")
      .joinAndMerge(gnomad_genomes_3_0, "gnomad_genomes_3_0", "left")
      .select(variants("*"), struct(col("1000_genomes"), col("topmed_bravo"), col("gnomad_genomes_2_1_1"), col("exac"), col("gnomad_genomes_3_0"), col("internal_frequencies") as "internal") as "frequencies")
      .drop("internal_frequencies")
  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val clinvar = spark.table("clinvar")
      .selectLocus($"clinvar.name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations")
    variants.joinAndMerge(clinvar, "clinvar", "left")

  }

  def joinWithDbSNP(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val dbsnp = spark.table("dbsnp")
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants("*"), dbsnp("name") as "dbsnp")
  }

  def joinWithDBNSFP(c: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val s = spark.table("dbnsfp_original")
      .withColumn("start", col("start").cast(LongType))
      .selectLocus(
        $"ensembl_transcript_id" as "ensembl_feature_id",
        struct(
          $"SIFT_converted_rankscore" as "sift_converted_rank_score",
          $"SIFT_pred" as "sift_pred",
          $"Polyphen2_HVAR_rankscore" as "polyphen2_hvar_score",
          $"Polyphen2_HVAR_pred" as "polyphen2_hvar_pred",
          $"FATHMM_converted_rankscore",
          $"FATHMM_pred" as "fathmm_pred",
          $"CADD_raw_rankscore" as "cadd_score",
          $"DANN_rankscore" as "dann_score",
          $"REVEL_rankscore" as "revel_rankscore",
          $"LRT_converted_rankscore" as "lrt_converted_rankscore",
          $"LRT_pred" as "lrt_pred") as "predictions",
        struct($"phyloP17way_primate_rankscore" as "phylo_p17way_primate_rankscore") as "conservations",
      )

    c
      .join(s, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(c("*"), s("predictions"), s("conservations"))


  }

}
