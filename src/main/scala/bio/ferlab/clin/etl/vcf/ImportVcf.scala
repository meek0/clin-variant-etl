package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportVcf extends SparkApp {

  val Array(_, batchId, runType, loadType) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  runType match {
    case "variants" => new Variants(batchId).run()
    case "consequences" => new Consequences(batchId, loadType).run()
    case "occurrences" => new Occurrences(batchId, loadType).run()
    case "all" =>
      new Occurrences(batchId, loadType).run()
      new Variants(batchId, loadType).run()
      new Consequences(batchId, loadType).run()
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
