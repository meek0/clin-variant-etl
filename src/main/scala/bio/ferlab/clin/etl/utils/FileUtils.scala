package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.fhir.GenomicFile
import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object FileUtils {
  /**
   * This function returns all S3 URLS for a given batchId. It filters files for a given [[GenomicFile]].
   * Table enriched_clinical is used to determine list of urls.
   *
   * @param batchId batchId of files
   * @param batchId batchId of files
   * @param file    used to filter documents
   * @param spark   spark session
   * @param conf    config
   * @return list of S3 URLs
   */
  def fileUrls(batchId: String, file: GenomicFile)(implicit spark: SparkSession, conf: Configuration): Set[FileInfo] = {
    import spark.implicits._
    val clinicalDf = conf.getDataset("enriched_clinical").read

    clinicalDf
      .where($"batch_id" === batchId)
      .select(
        explode($"${file.urlColumn}") as "url",
        $"aliquot_id",
        $"patient_id",
        $"specimen_id",
        $"service_request_id",
      )
      .as[FileInfo]
      .collect()
      .toSet
  }
}

case class FileInfo(url: String, aliquot_id: String, patient_id: String, specimen_id: String, service_request_id: String)