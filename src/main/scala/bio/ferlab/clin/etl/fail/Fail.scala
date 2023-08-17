package bio.ferlab.clin.etl.fail

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}
import org.slf4j

object Fail {
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  @main
  def run(rc: RuntimeETLContext): Unit = {
    log.info("This job will fail on purpose")
    throw new Exception("On purpose failure")
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
