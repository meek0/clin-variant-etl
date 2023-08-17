package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

abstract class PrepareCentric(rc: RuntimeETLContext, releaseId: String) extends SingleETL(rc) {
  override def loadSingle(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    loadForReleaseId(data, mainDestination, releaseId)
  }
}
