package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportExternal extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  println(s"Job: $jobName")


  jobName match {
    case "mane_summary" => new ManeSummaryETL().run(steps)
    case "panels" => new Panels().run(steps)
    case "refseq_annotation" => new RefSeqAnnotation().run(steps)
    case "refseq_feature" => new RefSeqFeature().run(steps)
    case "all" =>
      new Panels().run(steps)
    case s: String => throw new IllegalArgumentException(s"JobName [$s] unknown.")
  }

}
