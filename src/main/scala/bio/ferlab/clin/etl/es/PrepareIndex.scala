package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.RunType
import bio.ferlab.datalake.spark3.public.SparkApp

object PrepareIndex extends SparkApp {

  val Array(_, jobName, releaseId, runType) = args

  implicit val (conf, spark) = init()

  val rt = runType match {
    case "first_load" => RunType.FIRST_LOAD
    case "sample_load" => RunType.SAMPLE_LOAD
    case _ => RunType.INCREMENTAL_LOAD
  }

  jobName match {
    case "variants" => new PrepareVariantCentric(releaseId).run(rt)
  }

}

