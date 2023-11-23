package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.mainutils.Release
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class PrepareCnvCentric(rc: DeprecatedRuntimeETLContext, releaseId: String) extends PrepareCentric(rc, releaseId) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_cnv_centric")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      enriched_cnv.id -> enriched_cnv.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(enriched_cnv.id)

  }

}

object PrepareCnvCentric {
  @main
  def run(rc: DeprecatedRuntimeETLContext, release: Release): Unit = {
    PrepareCnvCentric(rc, release.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
