package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportVcf extends SparkApp {

  val Array(_, batchId, runType, chromosome) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  runType match {
    case "variants" => new Variants(batchId).run()
    case "consequences" => new Consequences(batchId).run()
    case "occurrences" => new Occurrences(batchId, chromosome).run()
    case "all" =>
      new Occurrences(batchId, chromosome).run()
      new Variants(batchId).run()
      new Consequences(batchId).run()
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
