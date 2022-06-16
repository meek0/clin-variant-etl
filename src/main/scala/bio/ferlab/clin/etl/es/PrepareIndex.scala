package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.spark3.public.SparkApp

object PrepareIndex extends SparkApp {

  val Array(_, _, jobName, releaseId) = args

  implicit val (conf, steps, spark) = init()

  log.info(s"Job: $jobName")
  log.info(s"releaseId: $releaseId")
  log.info(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "gene_centric" => new PrepareGeneCentric(releaseId).run(steps)
    case "gene_suggestions" => new PrepareGeneSuggestions(releaseId).run(steps)
    case "variant_centric" => new PrepareVariantCentric(releaseId).run(steps)
    case "variant_suggestions" => new PrepareVariantSuggestions(releaseId).run(steps)
    case "cnv_centric" => new PrepareCnvCentric(releaseId).run(steps)
  }

}

