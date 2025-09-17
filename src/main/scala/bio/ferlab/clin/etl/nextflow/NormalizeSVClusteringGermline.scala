package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.utils.FrequencyUtils.{FrequencyOps, pcNoFilter, pnCnv}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class NormalizeSVClusteringGermline(rc: RuntimeETLContext,
                                         sourceId: String, destinationId: String) extends NormalizeSVClustering(rc) {

  override val source: DatasetConf = conf.getDataset(sourceId)
  override val mainDestination: DatasetConf = conf.getDataset(destinationId)

  override def computeFrequencyRQDM(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val pn: Long = df.getPn(pnCnv)

    df
      .groupBy("name", "affected_status")
      .agg(
        firstAs("chromosome"),
        firstAs("start"),
        firstAs("end"),
        firstAs("reference"),
        firstAs("alternate"),
        firstAs("members"),
        collect_set("aliquot_id") as "aliquot_ids",
        when($"affected_status", pcNoFilter).otherwise(0) as "affected_pc",
        when(not($"affected_status"), pcNoFilter).otherwise(0) as "non_affected_pc",
      )
      .withColumn("total_pc", $"affected_pc" + $"non_affected_pc")
      .groupBy("name")
      .agg(
        firstAs("chromosome"),
        firstAs("start"),
        firstAs("end"),
        firstAs("reference"),
        firstAs("alternate"),
        firstAs("members"),
        array_distinct(flatten(collect_list($"aliquot_ids"))) as "aliquot_ids",
        struct(
          struct(
            struct(
              sum("affected_pc") as "pc",
              lit(pn) as "pn",
              coalesce(sum("affected_pc") / lit(pn), lit(0.0)) as "pf"
            ) as "affected",
            struct(
              sum("non_affected_pc") as "pc",
              lit(pn) as "pn",
              coalesce(sum("non_affected_pc") / lit(pn), lit(0.0)) as "pf"
            ) as "non_affected",
            struct(
              sum("total_pc") as "pc",
              lit(pn) as "pn",
              coalesce(sum("total_pc") / lit(pn), lit(0.0)) as "pf"
            ) as "total"
          ) as "germ"
        ) as "frequency_RQDM"
      )
  }
}

object NormalizeSVClusteringGermline {
  @main
  def run(rc: RuntimeETLContext, sourceId: String, destinationId: String): Unit = {
    NormalizeSVClusteringGermline(rc, sourceId, destinationId).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
