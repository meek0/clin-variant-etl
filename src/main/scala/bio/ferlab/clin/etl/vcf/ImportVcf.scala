package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.public.SparkApp

object ImportVcf extends SparkApp {

  val Array(_, _, batchId, jobName, chromosome) = args

  implicit val (conf, steps, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")


  println(s"batchId: $batchId")
  println(s"Job: $jobName")
  println(s"chromosome: $chromosome")
  println(s"runType: ${steps.mkString(" -> ")}")

  jobName match {
    case "variants" => new Variants(batchId, chromosome).run(steps)
    case "consequences" => new Consequences(batchId, chromosome).run(steps)
    case "occurrences" => new Occurrences(batchId, chromosome).run(steps)
    case "all" =>
      new Occurrences(batchId, chromosome).run(steps)
      new Variants(batchId, chromosome).run(steps)
      new Consequences(batchId, chromosome).run(steps)
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
