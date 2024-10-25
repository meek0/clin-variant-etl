package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunNextflow {

  // svclustering
  @main
  def prepare_svclustering(rc: RuntimeETLContext): Unit = PrepareSVClustering.run(rc)

  @main
  def normalize_svclustering(rc: RuntimeETLContext): Unit = NormalizeSVClustering.run(rc)

  // svclustering-parental-origin
  @main
  def prepare_svclustering_parental_origin(rc: RuntimeETLContext, batch: Batch): Unit = PrepareSVClusteringParentalOrigin.run(rc, batch)

  @main
  def normalize_svclustering_parental_origin(rc: RuntimeETLContext, batch: Batch): Unit = NormalizeSVClusteringParentalOrigin.run(rc, batch)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
