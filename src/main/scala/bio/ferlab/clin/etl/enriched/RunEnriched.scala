package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.public.SparkApp

object RunEnriched extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  println(s"Job: $jobName")
  println(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "variants" => new Variants().run(steps)
    case "consequences" => new Consequences().run(steps)
    case "all" =>
      new Variants().run(steps)
      new Consequences().run(steps)
    case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
  }

}

