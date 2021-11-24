package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{col, sha1}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class PrepareGeneCentric(releaseId: String)
                        (override implicit val conf: Configuration) extends ETL() {

  override val destination: DatasetConf = conf.getDataset("es_index_gene_centric")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      enriched_genes.id -> spark.table(s"${enriched_genes.table.get.fullName}")
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(enriched_genes.id)
      .withColumn("hash", sha1(col("symbol")))
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame =
    loadForReleaseId(data, destination, releaseId)
}

