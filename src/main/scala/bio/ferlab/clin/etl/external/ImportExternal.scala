package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportExternal extends SparkApp {

  val Array(_, jobName, runType) = args

  implicit val (conf, steps, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  println(s"Job: $jobName")


  jobName match {
    case "panels" => new Panels().run(steps)
    case "all" =>
      new Panels().run(steps)
    case s: String => throw new IllegalArgumentException(s"JobName [$s] unknown.")
  }

}
