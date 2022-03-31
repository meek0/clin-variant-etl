package bio.ferlab.clin.etl

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

package object es {

  def loadForReleaseId(data: DataFrame,
                       destination: DatasetConf,
                       releaseId: String)
                      (implicit conf: Configuration): DataFrame = {
    val updatedDestination = destination.copy(
      path = s"${destination.path}_${releaseId}",
      table = destination.table.map(t => t.copy(name = s"${t.name}_${releaseId}")))
    LoadResolver
      .write(data.sparkSession, conf)(destination.format, destination.loadtype)
      .apply(updatedDestination, data.repartition(50, col("chromosome")))
    data
  }

}
