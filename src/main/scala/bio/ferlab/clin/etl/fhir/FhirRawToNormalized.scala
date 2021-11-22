package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.commons.config.RunType
import bio.ferlab.datalake.spark3.etl.{ETL, RawToNormalizedETL}
import bio.ferlab.datalake.spark3.public.SparkApp

object FhirRawToNormalized extends SparkApp {

  val Array(_, jobName, runType) = args

  implicit val (conf, spark) = init()

  val rt = runType match {
    case "first_load" => RunType.FIRST_LOAD
    case "sample_load" => RunType.SAMPLE_LOAD
    case _ => RunType.INCREMENTAL_LOAD
  }

  val jobs: List[ETL] =
    FhirRawToNormalizedMappings
      .mappings
      .filter { case (_, dst, _) => (jobName == "all") || jobName == dst.id }
      .map { case (src, dst, transformations) => new RawToNormalizedETL(src, dst, transformations)}

  jobs.foreach(_.run(rt))

}
