package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.SparkSession

object RunEnriched extends SparkApp {

  val Array(_, jobName) = args

  implicit val (conf, spark) = init()

  run(jobName)

  def run(jobName: String = "all")(implicit spark: SparkSession): Unit = {
    jobName match {
      case "variants" => new Variants().run()
      case "consequences" => new Consequences().run()
      case "all" =>
        new Variants().run()
        new Consequences().run()
      case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
    }

  }


}

