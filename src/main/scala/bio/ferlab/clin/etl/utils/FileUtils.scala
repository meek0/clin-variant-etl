package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.fhir.GenomicFile
import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode

object FileUtils {

  private def fileUrlsBy(clinicalDf: DataFrame, file: GenomicFile, batchId : Option[String], analysisIds: Option[Seq[String]])(implicit spark: SparkSession, conf: Configuration) = {
    import spark.implicits._


    val clinicals = if (batchId.isDefined) clinicalDf.where($"batch_id" === batchId.get)
    else if (analysisIds.isDefined) clinicalDf.where($"analysis_id".isin(analysisIds.get: _*))
    else throw new IllegalArgumentException("Either batchId or analysisIds must be provided")

    clinicals.select(
        explode($"${file.urlColumn}") as "url",
        $"batch_id",
        $"analysis_id",
        $"aliquot_id",
        $"patient_id",
        $"specimen_id",
        $"sequencing_id",
        $"is_proband",
        $"mother_id",
        $"father_id",
      )
      .as[FileInfo]
      .collect()
      .toSet
  }

  /**
   * This function returns all S3 URLS for a given batchId. It filters files for a given [[GenomicFile]].
   * Table enriched_clinical is used to determine list of urls.
   *
   * @param batchId batchId of files
   * @param file    used to filter documents
   * @param spark   spark session
   * @param conf    config
   * @return list of S3 URLs
   */
  def fileUrls(batchId: String, file: GenomicFile)(implicit spark: SparkSession, conf: Configuration): Set[FileInfo] = {
    val clinicalDf = conf.getDataset("enriched_clinical").read

    fileUrlsBy(clinicalDf, file, Some(batchId), None)
  }

  def fileUrls(analysisIds: Seq[String], file: GenomicFile)(implicit spark: SparkSession, conf: Configuration): Set[FileInfo] = {
    val clinicalDf = conf.getDataset("enriched_clinical").read

    fileUrlsBy(clinicalDf, file, None, Some(analysisIds))
  }
}

case class FileInfo(batch_id: String, analysis_id: String, url: String, aliquot_id: String, patient_id: String, specimen_id: String, sequencing_id: String, is_proband: Boolean, mother_id: Option[String], father_id: Option[String])