package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.utils.FrequencyUtils.{FrequencyOps, pcNoFilter, pnCnv}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.{coalesce, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}


case class NormalizeSVClusteringSomatic(rc: RuntimeETLContext) extends NormalizeSVClustering(rc) {

  override val source: DatasetConf = conf.getDataset("nextflow_svclustering_somatic_output")
  override val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering_somatic")

  override def computeFrequencyRQDM(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val pn: Long = df.getPn(pnCnv)

    df
      .groupBy("name")
      .agg(
        firstAs("chromosome"),
        firstAs("start"),
        firstAs("end"),
        firstAs("reference"),
        firstAs("alternate"),
        firstAs("members"),
        pcNoFilter
      )
      .selectLocus(
        $"end",
        $"name",
        $"members",
        struct(
          struct(
            $"pc",
            lit(pn) as "pn",
            coalesce($"pc" / lit(pn), lit(0.0)) as "pf"
          ) as "som"
        ) as "frequency_RQDM"
      )
  }
}

object NormalizeSVClusteringSomatic {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    NormalizeSVClusteringSomatic(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
