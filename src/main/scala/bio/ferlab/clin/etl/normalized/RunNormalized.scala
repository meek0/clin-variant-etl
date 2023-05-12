package bio.ferlab.clin.etl.normalized

import bio.ferlab.datalake.spark3.SparkApp

object RunNormalized extends SparkApp {

  val Array(_, _, batchId, jobName) = args

  implicit val (conf, steps, spark) = init(appName = s"Normalize $jobName $batchId")

  log.info(s"batchId: $batchId")
  log.info(s"Job: $jobName")
  log.info(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "variants" => new Variants(batchId).run(steps)
    case "consequences" => new Consequences(batchId).run(steps)
    case "snv" => new SNV(batchId).run(steps)
    case "cnv" => new CNV(batchId).run(steps)
    case "panels" => new Panels().run(steps)
    case "all" =>
      new SNV(batchId).run(steps)
      new CNV(batchId).run(steps)
      new Variants(batchId).run(steps)
      new Consequences(batchId).run(steps)
      new Panels().run(steps)
      new Exomiser(batchId).run(steps)
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
