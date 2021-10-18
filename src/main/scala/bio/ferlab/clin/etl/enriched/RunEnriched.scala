package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.SparkSession

object RunEnriched extends SparkApp {

  val Array(_, runType) = args

  implicit val (conf, spark) = init()

  run(runType)

  def run(runType: String = "all")(implicit spark: SparkSession): Unit = {
    runType match {
      case "variants" => new Variants().run()
      case "consequences" => new Consequences().run()
      case "all" =>
        new Variants().run()
        new Consequences().run()
      case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
    }

  }


}

