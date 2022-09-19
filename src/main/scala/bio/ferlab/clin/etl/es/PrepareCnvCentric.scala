package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class PrepareCnvCentric(releaseId: String)(implicit configuration: Configuration) extends PrepareCentric(releaseId) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_cnv_centric")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      enriched_cnv.id -> enriched_cnv.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(enriched_cnv.id)

  }

}

