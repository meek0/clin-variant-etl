package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunNextflow {

  @main
  def prepare_svclustering_parental_origin(rc: RuntimeETLContext, batch: Batch): Unit = PrepareSVClusteringParentalOrigin.run(rc, batch)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
