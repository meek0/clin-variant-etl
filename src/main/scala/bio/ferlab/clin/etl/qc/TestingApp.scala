package bio.ferlab.clin.etl.qc

import bio.ferlab.clin.etl.qc.TestingApp.handleErrors
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

  lazy val normalized_snv: DataFrame = spark.table("normalized_snv")
  lazy val normalized_variants: DataFrame = spark.table("normalized_variants")
  lazy val variants: DataFrame = spark.table("variants")
  lazy val variant_centric = spark.table(s"variant_centric_$release_id")
  lazy val cnv_centric = spark.table(s"cnv_centric_$release_id")
  lazy val varsome: DataFrame = spark.table("varsome")

  def run(f: SparkSession => Unit): Unit = {
    spark.sql(s"use $database")
    f(spark)
  }

}

object TestingApp {
  def shouldBeEmpty(df: DataFrame, error: String): Unit = assert(df.count() == 0, error)

  def shouldNotContainNull(df: DataFrame, columnNames: String*): Option[String] = {
    val cols: Seq[String] = if (columnNames.nonEmpty) {
      columnNames.filter(colName => df.where(col(colName).isNull).count() > 0)
    } else {
      df.columns.toSeq
    }
    if (cols.nonEmpty) {
      Some(s"Column(s) ${cols.mkString(", ")} should not contain null")
    } else None
  }

  def combineErrors(errors: Option[String]*): Option[String] = {
    val filteredErrors = errors.flatten
    if (filteredErrors.nonEmpty) {
      Some(filteredErrors.mkString("\n"))
    } else None
  }

  def handleErrors(errors: Option[String]*): Unit = combineErrors(errors: _*).foreach(message => throw new IllegalStateException(message))
}