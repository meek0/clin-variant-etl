package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.SparkSession

object RunEnriched extends SparkApp {

  val Array(_, jobName, chromosome, loadType) = args

  implicit val (conf, spark) = init()

  run(jobName)

  def run(jobName: String = "all")(implicit spark: SparkSession): Unit = {
    jobName match {
      case "variants" => new Variants(chromosome, loadType).run()
      case "consequences" => new Consequences(chromosome, loadType).run()
      case "all" =>
        new Variants(chromosome, loadType).run()
        new Consequences(chromosome, loadType).run()
      case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
    }

  }


}

