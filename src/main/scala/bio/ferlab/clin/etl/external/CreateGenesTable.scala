package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.datalake.spark3.public.enriched.Genes

object CreateGenesTable extends SparkApp {

  val Array(_, _) = args

  // calls SparkApp.init() to load configuration file passed as first argument as well as an instance of SparkSession
  implicit val (conf, steps, spark) = init()

  spark.sql("use clin")

  new Genes().run(steps)
}
