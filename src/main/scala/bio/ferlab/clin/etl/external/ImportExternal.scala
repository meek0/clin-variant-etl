package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportExternal extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init(s"Import $jobName")

  spark.sparkContext.setLogLevel("ERROR")

  println(s"Job: $jobName")


  jobName match {
    case "mane-summary" => new ManeSummaryETL().run(steps)
    case "panels" => new Panels().run(steps)
    case "refseq-annotation" => new RefSeqAnnotation().run(steps)
    case "refseq-feature" => new RefSeqFeature().run(steps)
    case "all" =>
      new Panels().run(steps)
    case s: String => throw new IllegalArgumentException(s"JobName [$s] unknown.")
  }

}
