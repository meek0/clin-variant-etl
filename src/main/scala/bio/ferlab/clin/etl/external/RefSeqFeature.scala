package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class RefSeqFeature()(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("normalized_refseq_feature")
  val raw_refseq_feature: DatasetConf = conf.getDataset("raw_refseq_feature")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_refseq_feature.id -> raw_refseq_feature.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(raw_refseq_feature.id)
      .withColumnRenamed("# feature", "feature")
      .withColumn("start", $"start".cast("int"))
      .withColumn("end", $"end".cast("int"))
      .repartition(1)
  }

}

