package bio.ferlab.clin.etl.qc

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait TestingApp extends App {
  lazy val database = args(0)

  lazy val release_id = if (args.length == 2) args(1) else ""

  lazy val spark: SparkSession =
    SparkSession
      .builder
      .config(new SparkConf())
      .enableHiveSupport()
      .appName("TestingApp")
      .getOrCreate()

  lazy val cnv_centric = spark.table(s"cnv_centric_$release_id")
  lazy val gene_centric = spark.table(s"gene_centric_$release_id")
  lazy val normalized_snv: DataFrame = spark.table("normalized_snv")
  lazy val normalized_variants: DataFrame = spark.table("normalized_variants")
  lazy val variant_centric = spark.table(s"variant_centric_$release_id")
  lazy val variants: DataFrame = spark.table("variants")
  lazy val varsome: DataFrame = spark.table("varsome")

  lazy val gnomad_genomes_v2_1_1: DataFrame = spark.table("gnomad_genomes_v2_1_1")
  lazy val gnomad_exomes_v2_1_1: DataFrame = spark.table("gnomad_exomes_v2_1_1")
  lazy val gnomad_genomes_3_0: DataFrame = spark.table("gnomad_genomes_3_0")
  lazy val gnomad_genomes_v3: DataFrame = spark.table("gnomad_genomes_v3")
  lazy val fhir_clinical_impression: DataFrame = spark.table("fhir_clinical_impression")
  lazy val fhir_observation: DataFrame = spark.table("fhir_observation")
  lazy val fhir_organization: DataFrame = spark.table("fhir_organization")
  lazy val fhir_patient: DataFrame = spark.table("fhir_patient")
  lazy val fhir_practitioner: DataFrame = spark.table("fhir_practitioner")
  lazy val fhir_practitioner_role: DataFrame = spark.table("fhir_practitioner_role")
  lazy val fhir_service_request: DataFrame = spark.table("fhir_service_request")
  lazy val fhir_specimen: DataFrame = spark.table("fhir_specimen")
  lazy val fhir_task: DataFrame = spark.table("fhir_task")
  lazy val fhir_family: DataFrame = spark.table("fhir_family")
  lazy val normalized_cnv: DataFrame = spark.table("normalized_cnv")
  lazy val normalized_consequences: DataFrame = spark.table("normalized_consequences")
  lazy val normalized_panels: DataFrame = spark.table("normalized_panels")
  lazy val snv: DataFrame = spark.table("snv")
  lazy val cnv: DataFrame = spark.table("cnv")
  lazy val consequences: DataFrame = spark.table("consequences")
  lazy val gene_suggestions: DataFrame = spark.table(s"gene_suggestions_$release_id")
  lazy val variant_suggestions: DataFrame = spark.table(s"variant_suggestions_$release_id")
  lazy val thousand_genomes: DataFrame = spark.table("1000_genomes")
  lazy val clinvar: DataFrame = spark.table("clinvar")
  lazy val cosmic_gene_set: DataFrame = spark.table("cosmic_gene_set")
  lazy val dbsnp: DataFrame = spark.table("dbsnp")
  lazy val ddd_gene_set: DataFrame = spark.table("ddd_gene_set")
  lazy val ensembl_mapping: DataFrame = spark.table("ensembl_mapping")
  lazy val human_genes: DataFrame = spark.table("human_genes")
  lazy val hpo_gene_set: DataFrame = spark.table("hpo_gene_set")
  lazy val omim_gene_set: DataFrame = spark.table("omim_gene_set")
  lazy val orphanet_gene_set: DataFrame = spark.table("orphanet_gene_set")
  lazy val topmed_bravo: DataFrame = spark.table("topmed_bravo")
  lazy val refseq_annotation: DataFrame = spark.table("refseq_annotation")
  lazy val genes: DataFrame = spark.table("genes")
  lazy val dbnsfp_original: DataFrame = spark.table("dbnsfp_original")
  lazy val spliceai_indel: DataFrame = spark.table("spliceai_indel")
  lazy val spliceai_snv: DataFrame = spark.table("spliceai_snv")
  lazy val rare_variant_enriched: DataFrame = spark.table("rare_variant_enriched")

  def run(f: SparkSession => Unit): Unit = {
    spark.sql(s"use $database")
    f(spark)
  }
}

object TestingApp {
  def shouldBeEmpty(df: DataFrame): Option[String] = {
    if (df.count() > 0) Some("DataFrame should be empty") else None
  }

  def shouldNotBeEmpty(df: DataFrame, dfName: String): Option[String] = {
    if (df.head(1).isEmpty) Some(s"DataFrame ${dfName} should not be empty") else None
  }

  def shouldNotContainNull(df: DataFrame, columnNames: String*): Option[String] = {
    val columns: Seq[String] = if (columnNames.nonEmpty) columnNames else df.columns.toSeq
    val errorColumns: Seq[String] = columns.filter(colName => df.where(col(colName).isNull).count() > 0)
    if (errorColumns.nonEmpty) Some(s"Column(s) ${errorColumns.mkString(", ")} should not contain null") else None
  }

  def shouldNotContainOnlyNull(df: DataFrame, columnNames: String*): Option[String] = {
    val columns: Seq[String] = if (columnNames.nonEmpty) columnNames else df.columns.toSeq
    val errorColumns: Seq[String] = columns.filter(colName => df.where(col(colName).isNotNull).count() == 0)
    if (errorColumns.nonEmpty) Some(s"Column(s) ${errorColumns.mkString(", ")} should not contain only null") else None
  }

  def shouldNotContainSameValue(df: DataFrame, columnNames: String*): Option[String] = {
    val columns: Seq[String] = if (columnNames.nonEmpty) columnNames else df.columns.toSeq
    val errorColumns: Seq[String] = columns.filter(colName => df.select(col(colName)).na.drop.distinct().count() == 1)
    if (errorColumns.nonEmpty) Some(s"Column(s) ${errorColumns.mkString(", ")} should not contain same value") else None
  }

  def TestDfContainsAllVarFromBatch(df:org.apache.spark.sql.DataFrame, b:String, adAltFilter:Number, database: String)(implicit spark: SparkSession): Option[String] = {
    import spark.implicits._
    
    val bucket = database match {
      case "clin_qa"      => "cqgc-qa-app-files-import"
      case "clin_staging" => "cqgc-staging-app-files-import"
      case _              => "cqgc-prod-app-files-import"
    }

    val df_VCF = spark.read.format("vcf").load(s"s3a://$bucket/$b/$b.hard-filtered.formatted.norm.VEP.vcf.gz")
    .filter(!col("contigName").contains("_") && !col("contigName").contains("chrM"))
    .withColumn("start", $"start" + 1)
    .select($"contigName" as "chromosome", $"start", $"referenceAllele" as "reference", explode($"alternateAlleles") as "alternate", $"genotypes" as "donors")
    .select($"chromosome", $"start", $"reference", $"alternate", explode($"donors"))
    .select("*", "col.*").drop("col")
    .select($"chromosome", $"start", $"reference", $"alternate", $"sampleId" as "aliquot_id", $"alleleDepths"(1) as "ad_alt", $"calls")
    .filter(array_contains(col("calls"), 1) && $"ad_alt" >= adAltFilter && $"alternate" =!= "*")

    val df_ToTest = df
    .filter($"batch_id" === s"$b")
    .select($"chromosome", $"start", $"reference", $"alternate")
    .withColumn("chromosome", concat(lit("chr"), col("chromosome")))

    shouldBeEmpty(df_VCF.join(df_ToTest, Seq("chromosome", "start", "reference", "alternate"), "left_anti"))
  }

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)
  val includeFilter: Column = col("ad_alt") >= 3 && col("alternate") =!= "*"
  val frequencyFilter: Column = array_contains(col("filters"), "PASS") && includeFilter && col("gq") >= 20
  val ac: Column = sum(when(frequencyFilter, array_sum(filter(col("calls"), c => c === 1))).otherwise(0)) as "expected_ac"
  val pc: Column = sum(when(array_contains(col("calls"), 1) and frequencyFilter, 1).otherwise(0)) as "expected_pc"

  def combineErrors(errors: Option[String]*): Option[String] = {
    val filteredErrors = errors.flatten
    if (filteredErrors.nonEmpty) Some(filteredErrors.mkString("\n")) else None
  }

  def handleErrors(errors: Option[String]*): Unit = combineErrors(errors: _*).foreach(message => throw new IllegalStateException(message))
}