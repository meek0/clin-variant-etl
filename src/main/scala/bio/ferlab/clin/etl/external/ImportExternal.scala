package bio.ferlab.clin.etl.external

import bio.ferlab.clin.etl.vcf.Panels
import bio.ferlab.datalake.spark3.public.SparkApp

object ImportExternal extends SparkApp {

  val Array(_, jobName) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")
  
  println(s"Job: $jobName")

  jobName match {
    case "panels" => new Panels().run()
    case "all" =>
      new Panels().run()
    case s: String => throw new IllegalArgumentException(s"JobName [$s] unknown.")
  }

}
