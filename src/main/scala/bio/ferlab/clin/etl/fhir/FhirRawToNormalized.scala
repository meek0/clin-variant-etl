package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.spark3.etl.{ETL, RawToNormalizedETL}
import bio.ferlab.datalake.spark3.public.SparkApp

object FhirRawToNormalized extends SparkApp {

  implicit val (conf, spark) = init()

  val jobs: List[ETL] =
    FhirRawToNormalizedMappings
      .mappings
      .map { case (src, dst, transformations) => new RawToNormalizedETL(src, dst, transformations)}

  jobs.foreach(_.run())

}
