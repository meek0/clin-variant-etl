package bio.ferlab.clin.etl.script

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{arg, Flag, main, ParserForMethods}
import org.slf4j

object RunScript {
  val log: slf4j.Logger =
    slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  @main
  def rename_service_request_columns(
      rc: RuntimeETLContext,
      @arg(doc = "If specified, run a vacuum command on the datasets.")
      vacuum: Flag): Unit = {
    log.info("Running RenameServiceRequestColumns script")
    RenameServiceRequestColumns(rc).run(vacuum.value)
  }

  @main
  def update_partitioning(
      rc: RuntimeETLContext,
      @arg(doc = "If specified, run a vacuum command on the datasets.")
      vacuum: Flag,
      @arg(doc = "If specified, run the script in dry-run mode, without applying any changes.")
      dryrun: Flag): Unit = {
    log.info("Running UpdatePartitioning script")
    UpdatePartitioning(rc).run(vacuum.value, dryrun.value)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
