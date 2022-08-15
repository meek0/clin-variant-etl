package bio.ferlab.clin.etl.fail

import bio.ferlab.datalake.spark3.public.SparkApp

object Fail extends SparkApp {

  implicit val (conf, steps, spark) = init()

  log.info("This job will fail on purpose")

  throw new Exception("On purpose failure")
}
