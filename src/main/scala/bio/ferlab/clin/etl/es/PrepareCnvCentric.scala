package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class PrepareCnvCentric(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_cnv_centric")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      enriched_cnv.id -> enriched_cnv.read
    )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100), sortColumns = Seq("start"))

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minValue,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(enriched_cnv.id)
      // To prevent compatibility issues with the frontend, which still expects 'analysis_service_request_id' and 'service_request_id'
      .withColumnRenamed("analysis_id", "analysis_service_request_id")
      .withColumnRenamed("sequencing_id", "service_request_id")
  }

}

object PrepareCnvCentric {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    PrepareCnvCentric(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
