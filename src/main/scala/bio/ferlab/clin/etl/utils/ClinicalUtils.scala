package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object ClinicalUtils {

  def getAnalysisIdsInBatch(clinicalDf: DataFrame, batchId: String)
                                         (implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._

    clinicalDf.where($"batch_id" === batchId)
      .select("analysis_id")
      .distinct()
      .as[String]
      .collect()
  }
}
