package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.spark3.public.SparkApp

object PrepareIndex extends SparkApp {

  val Array(_, _, jobName, releaseId) = args

  implicit val (conf, steps, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  println(s"Job: $jobName")
  println(s"releaseId: $releaseId")
  println(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "gene_centric" => new PrepareGeneCentric(releaseId).run(steps)
    case "gene_suggestions" => new PrepareGeneSuggestions(releaseId).run(steps)
    case "variant_centric" => new PrepareVariantCentric(releaseId).run(steps)
    case "variant_suggestions" => new PrepareVariantSuggestions(releaseId).run(steps)
  }

}

