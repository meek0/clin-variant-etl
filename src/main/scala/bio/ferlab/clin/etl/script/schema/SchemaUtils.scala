package bio.ferlab.clin.etl.script.schema

import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import bio.ferlab.datalake.commons.config.{RunStep, RuntimeETLContext}
import bio.ferlab.datalake.spark3.utils.ErrorHandlingUtils.{optIfFailed, runOptThrow}

import org.apache.spark.sql.SparkSession
import org.slf4j


object SchemaUtils {

  implicit val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  private def logSummaryMessage(results: Iterable[(String, Option[Throwable])], summaryName: String, skipped: Int): Unit = {
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
       |""".stripMargin
    )
    if (nbFailed > 0) {
      log.warn(s"Failed datasets: ${failed.mkString(", ")}")
    }
  }

  /**
   * Find a list of datasets based on a given filter and execute an [[UpdateSchemaETL]]
   * @param rc          Runtime ETL context
   * @param filter      Filter to apply to the list of dataset ids
   * @param allMappings Map of all possible dataset ids -> transformations
   */
  def runUpdateSchemaFor(rc: RuntimeETLContext,
                         filter: String => Boolean,
                         allMappings: DatasetTransformationMapping): Unit =  runOptThrow {
    val (toProcess, skipped) = allMappings.mapping.keys.partition(filter)
    val results = toProcess.map { dsId =>
      log.info(s"Updating schema for: $dsId")
      val errorMessage = s"Failed to run $dsId"
      val ds = rc.config.getDataset(dsId)
      val dsTransformations = allMappings.mapping(dsId)
      val result = optIfFailed(new UpdateSchemaETL(rc, ds, dsTransformations).run(), errorMessage)
      (dsId, result)
    }
    val failed = results.collect { case (dsId, Some(_)) => dsId }
    logSummaryMessage(results, "Schema Update Summary", skipped.size)
    results.map(_._2)
  }

  /**
   * Run a vacuum operation on a list of datasets
   * @param rc                Runtime ETL context
   * @param datasetIds        List of dataset ids to vacuum
   * @param numberOfVersions  Number of versions to keep (default is 1)
   */
  def runVacuumFor(rc: RuntimeETLContext,
                   datasetIds: Seq[String],
                   numberOfVersions: Int = 1): Unit = runOptThrow {
    import bio.ferlab.datalake.spark3.utils.DeltaUtils.vacuum

    val results = datasetIds.map { dsId =>
      log.info(s"Applying vacuum on dataset: $dsId")
      val ds = rc.config.getDataset(dsId)
      val result = optIfFailed(vacuum(ds, numberOfVersions)(rc.spark, rc.config), s"Failed to vacuum dataset: $dsId")
      (dsId, result)
    }
    logSummaryMessage(results, "Vacuum Summary", 0)
    results.map(_._2)
  }
}
