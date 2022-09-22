package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.etl.v2.ETL
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

abstract class PrepareCentric(releaseId: String)(implicit configuration: Configuration) extends ETLSingleDestination {
  override def loadSingle(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now(),
                    repartition: DataFrame => DataFrame = defaultRepartition)(implicit spark: SparkSession): DataFrame = {
    loadForReleaseId(data, mainDestination, releaseId)
  }
}
