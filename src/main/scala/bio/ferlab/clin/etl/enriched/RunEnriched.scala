package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.public.SparkApp

object RunEnriched extends SparkApp {

  val Array(_, _, jobName, chromosome) = args

  implicit val (conf, steps, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  println(s"Job: $jobName")
  println(s"chromosome: $chromosome")
  println(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "variants" => new Variants(chromosome).run(steps)
    case "consequences" => new Consequences(chromosome).run(steps)
    case "all" =>
      new Variants(chromosome).run(steps)
      new Consequences(chromosome).run(steps)
    case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
  }

}

