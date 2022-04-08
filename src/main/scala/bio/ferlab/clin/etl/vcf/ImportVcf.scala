package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportVcf extends SparkApp {

  val Array(_, _, batchId, jobName) = args

  implicit val (conf, steps, spark) = init()

  log.info(s"batchId: $batchId")
  log.info(s"Job: $jobName")
  log.info(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "variants" => new Variants(batchId).run(steps)
    case "consequences" => new Consequences(batchId).run(steps)
    case "snv" => new SNV(batchId).run(steps)
    case "cnv" => new CNV(batchId).run(steps)
    case "all" =>
      new SNV(batchId).run(steps)
      new CNV(batchId).run(steps)
      new Variants(batchId).run(steps)
      new Consequences(batchId).run(steps)
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
