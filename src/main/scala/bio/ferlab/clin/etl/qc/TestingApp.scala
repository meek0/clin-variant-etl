package bio.ferlab.clin.etl.qc

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

trait TestingApp extends SparkApp {
  val database = args(0)

  val release_id = if (args.length == 2) args(1) else ""
  val spark: SparkSession =
    SparkSession
      .builder
      .config(new SparkConf())
      .enableHiveSupport()
      .appName("TestingApp")
      .getOrCreate()

  spark.sql(s"use $database")

  lazy val normalized_snv: DataFrame = spark.table("normalized_snv")
  lazy val variants: DataFrame = spark.table("variants")
  lazy val variant_centric = spark.table(s"varaint_centric_$release_id")

  def shouldBeEmpty(df: DataFrame, error: String): Unit = assert(df.count() == 0, error)
}