package bio.ferlab.clin.etl.qc

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def combineErrors(errors: Option[String]*): Option[String] = {
    val filteredErrors = errors.flatten
    if (filteredErrors.nonEmpty) Some(filteredErrors.mkString("\n")) else None
  }

  def handleErrors(errors: Option[String]*): Unit = combineErrors(errors: _*).foreach(message => throw new IllegalStateException(message))
}