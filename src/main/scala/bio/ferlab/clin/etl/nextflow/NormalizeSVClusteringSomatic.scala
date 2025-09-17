package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.utils.FrequencyUtils.{FrequencyOps, pcNoFilter, pnCnv}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.{coalesce, collect_set, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}


case class NormalizeSVClusteringSomatic(rc: RuntimeETLContext,
                                        sourceId: String, destinationId: String) extends NormalizeSVClustering(rc) {

  override val source: DatasetConf = conf.getDataset(sourceId)
  override val mainDestination: DatasetConf = conf.getDataset(destinationId)

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
        collect_set("aliquot_id") as "aliquot_ids",
        pcNoFilter
      )
      .selectLocus(
        $"end",
        $"name",
        $"members",
        $"aliquot_ids",
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
  def run(rc: RuntimeETLContext, sourceId: String, destinationId: String): Unit = {
    NormalizeSVClusteringSomatic(rc, sourceId, destinationId).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
