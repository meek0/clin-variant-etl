package bio.ferlab.clin.etl

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import org.apache.spark.sql.{DataFrame, SaveMode}

package object es {

  def loadForReleaseId(data: DataFrame,
                       destination: DatasetConf,
                       releaseId: String)
                      (implicit conf: Configuration): DataFrame = {
    data
      .write
      .partitionBy(destination.partitionby:_*)
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", s"${destination.rootPath}${destination.path}_${releaseId}")
      .saveAsTable(s"${destination.table.get.fullName}_${releaseId}")
    data
  }

}
