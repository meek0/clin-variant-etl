package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.mainutils.Destination
import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import mainargs.{ParserForMethods, main}

object FhirRawToNormalized {

  @main
  def run(rc: DeprecatedRuntimeETLContext, destination: Destination): Unit = {
    val jobs = FhirRawToNormalizedMappings
      .mappings(rc.config)
      .filter { case (_, dst, _) => (destination.id == "all") || destination.id == dst.id }
      .map { case (src, dst, transformations) => FhirToNormalizedETL(rc, src, dst, transformations) }

    jobs.foreach(_.run())
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
