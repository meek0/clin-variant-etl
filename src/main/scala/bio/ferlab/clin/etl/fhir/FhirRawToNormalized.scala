package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.spark3.etl.{ETL, RawToNormalizedETL}
import bio.ferlab.datalake.spark3.public.SparkApp

object FhirRawToNormalized extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init()

  val jobs: List[ETL] =
    FhirRawToNormalizedMappings
      .mappings
      .filter { case (_, dst, _) => (jobName == "all") || jobName == dst.id }
      .map { case (src, dst, transformations) =>
        dst.table.map(_.database).foreach(database => spark.sql(s"CREATE DATABASE IF NOT EXISTS $database"))
        new RawToNormalizedETL(src, dst, transformations)
      }

  jobs.foreach(_.run(steps))

}
