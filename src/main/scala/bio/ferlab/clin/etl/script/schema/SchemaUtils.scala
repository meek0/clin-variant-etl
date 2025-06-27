package bio.ferlab.clin.etl.script.schema

import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import bio.ferlab.datalake.commons.config.{Configuration, RunStep, RuntimeETLContext}
import bio.ferlab.datalake.spark3.utils.ErrorHandlingUtils.{optIfFailed, runOptThrow}
import org.apache.spark.sql.SparkSession

import io.delta.tables.DeltaTable
import org.slf4j

object SchemaUtils {

  implicit val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * Find a list of datasets based on a given filter and execute an [[UpdateSchemaETL]]
   * @param rc          Runtime ETL context
   * @param filter      Filter to apply to the list of dataset ids
   * @param allMappings Map of all possible dataset ids -> transformations
   */
  def runUpdateSchemaFor(rc: RuntimeETLContext,
                         filter: String => Boolean,
                         allMappings: DatasetTransformationMapping): Unit = runOptThrow {
    val (toProcess, skipped) = allMappings.mapping.keys.partition(filter)

    val results = toProcess.map { dsId =>
      log.info(s"Updating schema for: $dsId")
      val errorMessage = s"Failed to run $dsId"
      val ds = rc.config.getDataset(dsId)
      val dsTransformations = allMappings.mapping(dsId)
      val result = optIfFailed(new UpdateSchemaETL(rc, ds, dsTransformations).run(), errorMessage)
      (dsId, result)
    }
    logSummaryMessage(results.toSeq, "Schema Update Summary", skipped.size)
    results.map { case (ds_id, maybeError) => maybeError }
  }

  /**
   * Run a vacuum operation on a list of datasets
   * @param datasetIds        List of dataset ids to vacuum
   * @param numberOfVersions  Number of versions to keep (default is 1)
   * @param filter            Filter to apply to the dataset ids (default is to include all)
   */
  def runVacuumFor(
      datasetIds: Seq[String],
      numberOfVersions: Int = 1,
      filter: String => Boolean = _ => true)(implicit spark: SparkSession, conf: Configuration): Unit =
    runOptThrow {
      import bio.ferlab.datalake.spark3.utils.DeltaUtils.vacuum
      val (toVacuum, skipped) = datasetIds.partition(filter)

      val results = toVacuum.map { dsId =>
        log.info(s"Applying vacuum on dataset: $dsId")
        val ds = conf.getDataset(dsId)
        val result = optIfFailed(
          vacuum(ds, numberOfVersions)(spark, conf),
          s"Failed to vacuum dataset: $dsId"
        )
        (dsId, result)
      }
      logSummaryMessage(results, "Vacuum Summary", skipped.size)
      results.map { case (ds_id, maybeError) => maybeError }
    }

  /**
   * Updates the schema of each dataset using the provided transformation mapping, and optionally runs vacuum.
   * @param rc                Runtime ETL context
   * @param isUpdatedFilter   Filter to select dataset ids that require an update (skips already updated datasets)
   * @param mappings          DatasetTransformationMapping containing dataset ids and their corresponding transformations
   * @param vacuum            If true, vacuums the datasets after schema update
   * @param numberOfVersions  Number of versions to retain during vacuum (default: 1)
   */
  def runUpdateSchemaAndVacuum(rc: RuntimeETLContext,
                               isUpdatedFilter: String => Boolean,
                               mappings: DatasetTransformationMapping,
                               vacuum: Boolean = false,
                               numberOfVersions: Int = 1,
                               dryrun: Boolean = false): Unit = {
    implicit val spark: SparkSession = rc.spark
    implicit val conf: Configuration = rc.config

    val datasetIds = mappings.mapping.keys.toSeq
    val updateFilter: String => Boolean =
      dsId => isDeltaTable(dsId)(rc.spark, rc.config) && !isUpdatedFilter(dsId)
    if (dryrun) {
      logDryRunMessage(datasetIds.filter(updateFilter), "Update Schema")
    } else {
      runUpdateSchemaFor(rc, updateFilter, mappings)
    }
    if (vacuum) {
      val vacuumFilter: String => Boolean = dsId => isDeltaTable(dsId)
      if (dryrun) {
        logDryRunMessage(datasetIds.filter(vacuumFilter), "Vacuum")

      } else {
        runVacuumFor(datasetIds, numberOfVersions, vacuumFilter)
      }
    }
  }

  private def logSummaryMessage(results: Seq[(String, Option[Throwable])],
                                summaryName: String,
                                skipped: Int): Unit = {
    val nbProcessed = results.size
    val failed = results.filter(_._2.isDefined)
    val nbFailed = failed.size
    val nbSuccessful = nbProcessed - nbFailed
    log.info(s"""
       |$summaryName:
       |  - Skipped: $skipped
       |  - Processed: $nbProcessed
       |      - Successful: $nbSuccessful
       |      - Failed: $nbFailed
       |""".stripMargin)
    if (nbFailed > 0) {
      log.warn(s"Failed datasets: ${failed.mkString(", ")}")
    }
  }

  private def isDeltaTable(datasetId: String)(implicit
      spark: SparkSession,
      conf: Configuration): Boolean = {
    val ds = conf.getDataset(datasetId)
    DeltaTable.isDeltaTable(spark, ds.location(conf))
  }

  private def logDryRunMessage(datasetIds: Seq[String], operation: String): Unit = {
    val datasetIdsStr = datasetIds.mkString("\n  ")
    log.info(s"Dry Run: $operation will be applied on the following datasets:\n  $datasetIdsStr")
  }
}
