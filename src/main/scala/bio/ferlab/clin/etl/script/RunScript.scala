package bio.ferlab.clin.etl.script

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{Flag, ParserForMethods, arg, main}
import org.slf4j

object RunScript {
  val log: slf4j.Logger =
    slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  @main
  def rename_service_request_columns(rc: RuntimeETLContext,
                                     @arg(doc = "If specified, run a vacuum command on the datasets.") vacuum: Flag): Unit = {
    log.info("Running RenameServiceRequestColumns script")
    RenameServiceRequestColumns(rc).run(vacuum.value)
  }

  @main
  def repartition_franklin_by_analysis_id(rc: RuntimeETLContext,
                                          @arg(doc = "If specified, run a vacuum command on the datasets.") vacuum: Flag,
                                          @arg(doc = "If specified, run the script in dry-run mode, without applying any changes.") dryrun: Flag,
                                          @arg(name = "exclude", short = 'e', doc = "Exclude this analysis id from the clinical data") exclude: Seq[String]): Unit = {
    log.info("Running RepartitionFranklinByAnalysisId script")
    RepartitionFranklinByAnalysisId(rc, exclude).run(vacuum.value, dryrun.value)
  }

  @main
  def franklin_folder_migration(rc: RuntimeETLContext,
                                @arg(doc = "If specified, run the script in dry-run mode, without applying any changes.") dryrun: Flag): Unit = {
    log.info("Running FranklinFolderMigration script")
    FranklinFolderMigration(rc).run(dryrun.value)
  }

  @main
  def repartition_by_analysis_id(rc: RuntimeETLContext,
                                 @arg(doc = "If specified, run a vacuum command on the datasets.") vacuum: Flag,
                                 @arg(doc = "If specified, run the script in dry-run mode, without applying any changes.") dryrun: Flag): Unit = {
    log.info("Running RepartitionByAnalysisId script")
    RepartitionByAnalysisId(rc).run(vacuum.value, dryrun.value)
  }

  // This script updates the partitioning of configured datasets to match the current configuration.
  @main
  def update_partitioning(rc: RuntimeETLContext,
                          @arg(doc = "If specified, run a vacuum command on the datasets.") vacuum: Flag,
                          @arg(doc = "If specified, run the script in dry-run mode, without applying any changes.") dryrun: Flag): Unit = {
    log.info("Running UpdatePartitioning script")
    new UpdatePartitioning(rc).run(vacuum.value, dryrun.value)
  }

  @main
  def drop_url_columns(rc: RuntimeETLContext): Unit = {
    log.info("Running DropUrlColumns script")
    DropUrlColumns.run(rc)
  }


  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
