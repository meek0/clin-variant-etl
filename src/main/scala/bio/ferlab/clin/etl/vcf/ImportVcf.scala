package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportVcf extends SparkApp {

  val Array(_, batchId, jobName, chromosome, loadType) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  jobName match {
    case "variants" => new Variants(batchId, chromosome, loadType).run()
    case "consequences" => new Consequences(batchId, chromosome, loadType).run()
    case "occurrences" => new Occurrences(batchId, chromosome, loadType).run()
    case "all" =>
      new Occurrences(batchId, loadType).run()
      new Variants(batchId, loadType).run()
      new Consequences(batchId, loadType).run()
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
