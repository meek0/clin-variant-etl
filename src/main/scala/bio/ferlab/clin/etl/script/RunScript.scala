package bio.ferlab.clin.etl.script

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}
import org.slf4j

object RunScript {
  val log: slf4j.Logger =
    slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  // Feel free to remove this test entrypoint once a real entrypoint is created
  @main
  def test(rc: RuntimeETLContext): Unit = {
    log.info("This is a test script")
    log.info(rc.toString())
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
