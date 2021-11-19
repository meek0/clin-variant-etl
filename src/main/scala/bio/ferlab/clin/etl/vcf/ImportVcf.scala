package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.commons.config.RunType
import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object ImportVcf extends SparkApp {

  val Array(_, batchId, jobName, chromosome, runType) = args

  implicit val (conf, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  val rt = runType match {
    case "first_load" => RunType.FIRST_LOAD
    case "sample_load" => RunType.SAMPLE_LOAD
    case _ => RunType.INCREMENTAL_LOAD
  }

  println(s"batchId: $batchId")
  println(s"Job: $jobName")
  println(s"chromosome: $chromosome")
  println(s"runType: $rt")

  jobName match {
    case "variants" => new Variants(batchId, chromosome).run()
    case "consequences" => new Consequences(batchId, chromosome).run()
    case "occurrences" => new Occurrences(batchId, chromosome).run()
    case "all" =>
      new Occurrences(batchId, chromosome).run(rt)
      new Variants(batchId, chromosome).run(rt)
      new Consequences(batchId, chromosome).run(rt)
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
