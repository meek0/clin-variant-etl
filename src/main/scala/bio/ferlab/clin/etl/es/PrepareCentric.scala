package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

abstract class PrepareCentric(rc: DeprecatedRuntimeETLContext, releaseId: String) extends SingleETL(rc) {
  override def loadSingle(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    loadForReleaseId(data, mainDestination, releaseId)
  }
}
