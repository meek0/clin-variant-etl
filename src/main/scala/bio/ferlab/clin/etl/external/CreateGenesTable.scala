package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.spark3.public.{ImportGenesTable, SparkApp}

object CreateGenesTable extends SparkApp {

  val Array(_) = args

  // calls SparkApp.init() to load configuration file passed as first argument as well as an instance of SparkSession
  implicit val (conf, spark) = init()

  spark.sql("use clin")

  new ImportGenesTable().run()
}
