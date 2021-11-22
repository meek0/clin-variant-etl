package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.RunType
import bio.ferlab.datalake.spark3.public.SparkApp

object RunEnriched extends SparkApp {

  val Array(_, jobName, chromosome, runType) = args

  implicit val (conf, spark) = init()

  val rt = runType match {
    case "first_load" => RunType.FIRST_LOAD
    case "sample_load" => RunType.SAMPLE_LOAD
    case _ => RunType.INCREMENTAL_LOAD
  }

  jobName match {
    case "variants" => new Variants(chromosome).run(rt)
    case "consequences" => new Consequences(chromosome).run(rt)
    case "all" =>
      new Variants(chromosome).run(rt)
      new Consequences(chromosome).run(rt)
    case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
  }

}

