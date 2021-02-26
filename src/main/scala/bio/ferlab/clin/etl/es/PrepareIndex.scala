package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.utils.GenomicsUtils._
import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PrepareIndex extends App {

  val Array(output, batchId) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Prepare Index").getOrCreate()

  run(output, batchId)
  runUpdate(output, batchId)

  def run(output: String, lastExecutionDateTime: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val newVariantFlow: DataFrame => DataFrame = (buildNewVariants _)
      .andThen(joinWithPopulations)
      .andThen(joinWithClinvar)
      .andThen(joinWithDbSNP)
      .andThen(joinWithGenes)
      .andThen(addExtDb)

    val newVariants =
      spark.table("variants")
        .where(col("createdOn") >= lastExecutionDateTime)

    val finalDf = newVariantFlow(newVariants)
    finalDf
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/extract")

    finalDf
  }

  def runUpdate(output: String, lastExecutionDateTime: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val updateVariantFlow: DataFrame => DataFrame = buildNewVariants _

    val updatedVariants =
      spark.table("variants")
        .where(col("updatedOn") >= lastExecutionDateTime and col("createdOn") =!= col("updatedOn"))

    val finalDf = updateVariantFlow(updatedVariants)
      .withColumn("frequencies", map(lit("internal"), col("internal_frequencies")))
      .select("chromosome", "start", "reference", "alternate", "donors", "lab_frequencies", "frequencies", "participant_number")

    finalDf
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/update")
    finalDf
  }

  private def buildNewVariants(variantDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val newVariants = variantDF
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

    joinWithConsequences
      .joinByLocus(occurrences, "inner")
      .groupBy(locus :+ col("alternate") :+ col("organization_id"): _*)
      .agg(ac, an, het, hom, participant_number,
        first(struct(joinWithConsequences("*"), $"variant_type")) as "variant",
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
