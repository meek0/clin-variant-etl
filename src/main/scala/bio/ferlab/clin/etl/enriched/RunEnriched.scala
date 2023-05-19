package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.SparkApp

object RunEnriched extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init(appName = s"Enrich $jobName")

  log.info(s"Job: $jobName")
  log.info(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "variants" => new Variants().run(steps)
    case "consequences" => new Consequences().run(steps)
    case "cnv" => new CNV().run(steps)
    case "snv" => new SNV().run(steps)
    case "all" =>
      new Variants().run(steps)
      new Consequences().run(steps)
      new CNV().run(steps)
      new SNV().run(steps)
    case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
  }

}

