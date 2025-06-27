package bio.ferlab.clin.testutils

import bio.ferlab.datalake.commons.config.{DatasetConf, Configuration}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadResolverUtils {

  def write(datasetConf: DatasetConf, df: DataFrame)(implicit spark: SparkSession, conf: Configuration): Unit = {
    LoadResolver
      .write
      .apply(datasetConf.format, datasetConf.loadtype)
      .apply(datasetConf, df)
  }
}
