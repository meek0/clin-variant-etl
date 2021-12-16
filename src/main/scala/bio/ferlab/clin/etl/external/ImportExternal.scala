package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.commons.config.RunType
import bio.ferlab.datalake.spark3.public.SparkApp

object ImportExternal extends SparkApp {

  val Array(_, jobName, runType) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  //TODO include the following code in [[SparkApp]]
  val rt = runType match {
    case "first_load" => RunType.FIRST_LOAD
    case "sample_load" => RunType.SAMPLE_LOAD
    case _ => RunType.INCREMENTAL_LOAD
  }

  println(s"Job: $jobName")


  jobName match {
    case "panels" => new Panels().run(rt)
    case "all" =>
      new Panels().run(rt)
    case s: String => throw new IllegalArgumentException(s"JobName [$s] unknown.")
  }

}
