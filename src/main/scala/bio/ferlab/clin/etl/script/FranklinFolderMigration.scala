package bio.ferlab.clin.etl.script

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.commons.file.{File, FileSystem, FileSystemResolver}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.utils.ErrorHandlingUtils.{optIfFailed, runOptThrow}
import org.apache.spark.sql.SparkSession
import org.slf4j

case class FranklinFolderMigration(rc: RuntimeETLContext) {

  implicit val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def run(dryrun: Boolean = false): Unit = {

    implicit val spark: SparkSession = rc.spark
    implicit val conf: Configuration = rc.config
    import spark.implicits._

    val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")
    val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")
    val franklinBasePath = raw_franklin.rootPath + "/raw/landing/franklin/"
    val fs = FileSystemResolver.resolve(conf.getStorage(raw_franklin.storageid).filesystem)

    val franklinToClinAnalysisId = normalized_franklin.read
      .select("franklin_analysis_id", "analysis_id").distinct()
      .as[(String, String)].collect().toMap

    val batchFolders = fs.list(franklinBasePath, false).filter(file => file.path.contains("batch_id") && file.isDir)
    runOptThrow {
      batchFolders.map { batchFolder =>
        optIfFailed(
          processBatch(dryrun, fs, franklinToClinAnalysisId, batchFolder),
          s"Failed to process batch folder: ${batchFolder.path}"
        )
      }
    }
  }

  private def processBatch(dryrun: Boolean, fs: FileSystem, franklinToClinAnalysisId: Map[String, String], batchFolder: File): Unit = {
    val dryrunPrefix = if (dryrun) "(DRYRUN) " else ""

    log.info(s"${dryrunPrefix}Processing ${batchFolder.name}")
    val franklinBasePath = batchFolder.path.replace(batchFolder.name, "")

    fs.list(batchFolder.path, true).filter(file => !file.isDir).foreach { file =>
      if (!file.path.endsWith(".json")) {
        throw new IllegalStateException(s"${file.path}: not a .json file. This may indicate an incomplete or irregular batch. Please inspect and migrate manually if needed. Note: you may need to delete any folders copied by this migration, as it assumes all batches are complete.")
      }

      val partitionsSubFolders = file.path.replace(franklinBasePath, "").split("/")
      val aliquotId = partitionsSubFolders(2).split("=").last
      val franklinAnalysisId = partitionsSubFolders(3).split("=").last
      val analysisId = franklinToClinAnalysisId(franklinAnalysisId)

      val destination = franklinBasePath + "analysis_id=" + analysisId + "/aliquot_id=" + aliquotId + "/franklin_analysis_id=" + franklinAnalysisId + "/" + file.name
      if (fs.exists(destination)) {
        log.info(s"${dryrunPrefix}Skipping ${file.path}, destination already exists: $destination")
      } else {
        log.info(s"${dryrunPrefix}Copying ${file.path} to $destination")
        if (!dryrun) {
          fs.copy(file.path, destination, false)
        }
      }
    }
  }

}
