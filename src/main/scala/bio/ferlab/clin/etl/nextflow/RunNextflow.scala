package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunNextflow {

  @main
  def normalize_svclustering_germline_del(rc: RuntimeETLContext): Unit = {
    val sourceId = "nextflow_svclustering_germline_del_output"
    val destinationId = "normalized_svclustering_germline_del"
    NormalizeSVClusteringGermline.run(rc, sourceId, destinationId)
  }

  @main
  def normalize_svclustering_germline_dup(rc: RuntimeETLContext): Unit = {
    val sourceId = "nextflow_svclustering_germline_dup_output"
    val destinationId = "normalized_svclustering_germline_dup"
    NormalizeSVClusteringGermline.run(rc, sourceId, destinationId)
  }

  @main
  def normalize_svclustering_somatic_del(rc: RuntimeETLContext): Unit = {
    val sourceId = "nextflow_svclustering_somatic_del_output"
    val destinationId = "normalized_svclustering_somatic_del"
    NormalizeSVClusteringSomatic.run(rc, sourceId, destinationId)
  }

  @main
  def normalize_svclustering_somatic_dup(rc: RuntimeETLContext): Unit = {
    val sourceId = "nextflow_svclustering_somatic_dup_output"
    val destinationId = "normalized_svclustering_somatic_dup"
    NormalizeSVClusteringSomatic.run(rc, sourceId, destinationId)
  }

  @main
  def prepare_svclustering_parental_origin(rc: RuntimeETLContext, batch: Batch): Unit = PrepareSVClusteringParentalOrigin.run(rc, batch)

  @main
  def normalize_svclustering_parental_origin(rc: RuntimeETLContext, batch: Batch): Unit = NormalizeSVClusteringParentalOrigin.run(rc, batch)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
