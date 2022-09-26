package bio.ferlab.clin.etl.qc

import org.apache.spark.SparkConf
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
  lazy val varsome: DataFrame = spark.table("varsome")
  def shouldBeEmpty(df: DataFrame, error: String): Unit = assert(df.count() == 0, error)

  def run(f: SparkSession => Unit): Unit = {
    spark.sql(s"use $database")
    f(spark)
  }
}