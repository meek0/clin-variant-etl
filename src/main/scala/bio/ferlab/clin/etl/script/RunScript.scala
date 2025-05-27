package bio.ferlab.clin.etl.script

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{arg, Flag, main, ParserForMethods}
import org.slf4j

object RunScript {
  val log: slf4j.Logger =
    slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * With mainargs, if there is only one @main method in the object,
   * it is not possible to specify the entrypoint name on the command line.
   * Keeping this dummy/test method allows specifying the entrypoint
   * explicitly when running the script. If you add a second real @main
   * method, you can remove this test method.
  */
  @main
  def test(rc: RuntimeETLContext): Unit = {
    log.info("This is a test script")
    log.info(rc.toString())
  }

  @main
  def rename_service_request_columns(
    rc: RuntimeETLContext,
    @arg(doc = "If this flag is specified, the script will run a vacuum command on the different datasets.")
    vacuum: Flag): Unit = {
    log.info("Running RenameServiceRequestColumns script")
    new RenameServiceRequestColumns(rc).run(vacuum.value)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
