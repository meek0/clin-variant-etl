package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.SparkSession

object PrepareIndex extends SparkApp {

  val Array(_, jobName, releaseId, loadType) = args

  implicit val (conf, spark) = init()

  run(jobName)

  def run(jobName: String = "all")(implicit spark: SparkSession): Unit = {
    jobName match {
      case "variants" => new PrepareVariantCentric(releaseId).run()
    }
  }


}

