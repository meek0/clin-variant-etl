package bio.ferlab.clin.etl.utils

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileUtils {
  /**
   * This function returns all S3 URLS for a given batchId. It filters files for given format and data type.
   * Tables normalized_tasks and normalized_document_reference are used to determine list of urls.
   *
   * @param batchId  batchId of files
   * @param dataType used to filter documents
   * @param format   use to filter documents
   * @param spark    spark session
   * @param c        config
   * @return list of S3 URLs
   */
  def filesUrl(batchId: String, dataType: String, format: String)(implicit spark: SparkSession, c: Configuration): Set[FileInfo] = {
    val tasks = c.getDataset("normalized_task").read
    val documentReferences = c.getDataset("normalized_document_reference").read
    filesUrlFromDF(tasks, documentReferences, batchId, dataType, format)
  }

  def filesUrlFromDF(tasks: DataFrame, documentReferences: DataFrame, batchId: String, dataType: String, format: String)(implicit spark: SparkSession): Set[FileInfo] = {
    import spark.implicits._
    val documentIdsFromTasks = tasks
      .where(col("batch_id") === batchId)
      .select(explode(col("documents")) as "document", col("experiment.aliquot_id") as "aliquot_id")
      .where(col("document.document_type") === dataType)
      .select(col("document.id") as "id", col("aliquot_id") )
    val documents = documentReferences
      .where(col("type") === dataType)
      .select(col("id"), explode(col("contents")) as "content", col("patient_id"), col("specimen_id"))
      .where(col("content.format") === format)
      .join(documentIdsFromTasks, Seq("id"), "inner")
      .select(col("content.s3_url") as "url", col("aliquot_id"), col("patient_id"), col("specimen_id"))
    documents.as[FileInfo].collect().toSet
  }


}

case class FileInfo(url: String, aliquot_id: String, patient_id: String, specimen_id: String)