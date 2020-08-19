package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.ByLocus._
import bio.ferlab.clin.etl.columns.{ac, an}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PrepareIndex extends App {

  val Array(output, batchId) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Prepare Index").getOrCreate()

  run(output, batchId)

  def run(output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    spark.sql("use clin")

    val joinVariants: String => DataFrame = (buildNewVariants _)
      .andThen(joinWithPopulations)
      .andThen(joinWithClinvar)
      .andThen(joinWithDbSNP)
      .andThen(joinWithGenes)
    joinVariants(batchId)
      .write.mode("overwrite")
      .json(s"$output/extract")

    //    val updatedVariants = spark.table("variants").where($"last_batch_id" === batchId)
    //      .join(occurrences, newVariants("chromosome") === occurrences("chromosome") && newVariants("start") === occurrences("start") && newVariants("reference") === occurrences("reference") && newVariants("alternate") === occurrences("alternate"))


  }

  private def buildNewVariants(batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val newVariants = spark.table("variants").where($"batch_id" === batchId).withColumnRenamed("genes", "genes_symbol").as("variants")
    val consequences = buildConsequences(spark)

    val joinWithConsequences = newVariants
      .joinAndDrop(consequences)
      .groupByLocus()
      .agg(
        first(struct("variants.*")) as "variant",
        collect_list(struct("consequences.*")) as "consequences")
      .select($"variant.*",
        $"consequences")

    val occurrences = spark.table("occurrences")
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end").where($"has_alt" === true)
      .as("occurrences")

    joinWithConsequences
      .joinAndDrop(occurrences)
      .groupByLocus()
      .agg(
        first(struct(joinWithConsequences("*"))) as "variant",
        collect_list(struct("occurrences.*")) as "occurrences",
        struct(ac, an) as "internal_frequencies"
      )
      .select($"variant.*",
        $"occurrences",
        $"internal_frequencies"
      )

  }

  private def buildConsequences(implicit spark: SparkSession) = {
    val csq = spark.table("consequences")
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_transcript_id", "ensembl_regulatory_id")
      .as("consequences")

    joinWithDBNSFP(csq)
  }

  def joinWithGenes(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val genes = spark.table("genes")
    variants
      .join(genes, variants("chromosome") === genes("chromosome") && array_contains(variants("genes_symbol"), genes("symbol")), "left")
      .drop(genes("chromosome"))
      .groupByLocus()
      .agg(
        first(struct(variants("*"))) as "variant",
        collect_list(struct("genes.*")) as "genes"
      )
      .select("variant.*", "genes")
  }

  def joinWithPopulations(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val genomes = spark.table("1000_genomes").as("1000Gp3").selectLocus($"ac", $"af")
    val topmed = spark.table("topmed_bravo").selectLocus($"ac", $"af")
    val gnomad_genomes_2_1 = spark.table("gnomad_genomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af")
    val gnomad_exomes_2_1 = spark.table("gnomad_exomes_2_1_1_liftover_grch38").selectLocus($"ac", $"af")
    val gnomad_genomes_3_0 = spark.table("gnomad_genomes_3_0").as("gnomad_genomes_3_0").selectLocus($"ac", $"af")

    variants.joinAndMerge(genomes, "1000_genomes", "left")
      .joinAndMerge(topmed, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_2_1, "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(gnomad_exomes_2_1, "exac", "left")
      .joinAndMerge(gnomad_genomes_3_0, "gnomad_genomes_3_0", "left")
      .select(variants("*"), struct(col("1000_genomes"), col("gnomad_genomes_2_1_1"), col("exac"), col("gnomad_genomes_3_0"), col("internal_frequencies") as "internal") as "frequencies")
      .drop("internal_frequencies")
  }

  def joinWithClinvar(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val clinvar = spark.table("clinvar").selectLocus($"clinvar.name" as "clinvar_id", $"clin_sig")
    variants.joinAndMerge(clinvar, "clinvar", "left")

  }

  def joinWithDbSNP(variants: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val dbsnp = spark.table("dbsnp")
    variants.joinAndDrop(dbsnp, "left")
      .select(variants("*"), dbsnp("name") as "dbsnp")
  }

  def joinWithDBNSFP(c: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val s = spark.table("dbnsfp_scores")
      .selectLocus(
        $"ensembl_transcript_id",
        struct($"sift_converted_rank_score", $"sift_pred",
          $"polyphen2_hvar_score", $"polyphen2_hvar_pred",
          $"fathmm_converted_rank_score", $"fathmm_pred",
          $"cadd_score", $"dann_score", $"revel_rankscore",
          $"lrt_converted_rankscore", $"lrt_pred") as "prediction_scores",
        struct($"phylo_p17way_primate_rankscore") as "conservations_scores",
      )

    c.join(s,
      c("chromosome") === s("chromosome") &&
        c("start") === s("start") &&
        c("reference") === s("reference") &&
        c("alternate") === s("alternate") &&
        c("ensembl_feature_id") === s("ensembl_transcript_id"),
      "left")
      .drop(s("chromosome"))
      .drop(s("start"))
      .drop(s("reference"))
      .drop(s("alternate"))
      .drop(s("ensembl_transcript_id"))

      .select(c("*"), s("prediction_scores"), s("conservations_scores"))


  }

}

