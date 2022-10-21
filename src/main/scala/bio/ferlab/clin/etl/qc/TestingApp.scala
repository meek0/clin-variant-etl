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
  lazy val normalized_snv: DataFrame = spark.table("normalized_snv")
  lazy val normalized_variants: DataFrame = spark.table("normalized_variants")
  lazy val variant_centric = spark.table(s"variant_centric_$release_id")
  lazy val variants: DataFrame = spark.table("variants")
  lazy val varsome: DataFrame = spark.table("varsome")

  def run(f: SparkSession => Unit): Unit = {
    spark.sql(s"use $database")
    f(spark)
  }
}

object TestingApp {
  def shouldBeEmpty(df: DataFrame): Option[String] = {
    if (df.count() > 0) Some("DataFrame should be empty") else None
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