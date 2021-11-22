package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.spark3.etl.{ETL, RawToNormalizedETL}
import bio.ferlab.datalake.spark3.public.SparkApp

object FhirRawToNormalized extends SparkApp {

  val Array(_, jobName, runType) = args

  implicit val (conf, spark) = init()

  val jobs: List[ETL] =
    FhirRawToNormalizedMappings
      .mappings
      .filter { case (_, dst, _) => (jobName == "all") || jobName == dst.id }
      .map { case (src, dst, transformations) => new RawToNormalizedETL(src, dst, transformations)}

  if (runType == "first_load")
    jobs.foreach(_.reset())

  jobs.foreach(_.run())

}
