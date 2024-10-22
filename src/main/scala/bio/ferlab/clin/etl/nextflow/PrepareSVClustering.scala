package bio.ferlab.clin.etl.nextflow

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.CsvUtils
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.regexp_replace

import java.time.LocalDateTime

case class PrepareSVClustering(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  override val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering_input")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(enriched_clinical.id -> enriched_clinical.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    data(enriched_clinical.id)
      .where($"cnv_vcf_urls".isNotNull)
      .select(
        $"aliquot_id" as "sample",
        $"analysis_service_request_id" as "familyId",
        regexp_replace($"cnv_vcf_urls"(0), "s3a://", "s3://") as "vcf" // There's always a single file
      )
      .distinct()
  }

  override def loadSingle(data: DataFrame,
                          lastRunValue: LocalDateTime,
                          currentRunValue: LocalDateTime): DataFrame = {
    super.loadDataset(data, mainDestination)
    CsvUtils.renameCsvFile(mainDestination)
  }
}

object PrepareSVClustering {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    PrepareSVClustering(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
