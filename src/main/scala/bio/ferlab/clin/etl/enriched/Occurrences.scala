package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.RunType.FIRST_LOAD
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class Occurrences(chromosome: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_occurrences")
  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_occurrences")

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    runType match {
      case FIRST_LOAD =>
        FileSystemResolver.resolve(conf.getStorage(destination.storageid).filesystem).remove(destination.location)
        destination.table.foreach(t => spark.sql(s"DROP TABLE IF EXISTS ${t.fullName}"))
        run(minDateTime, LocalDateTime.now())
      case _ =>
        run(minDateTime, LocalDateTime.now())
    }
  }

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_occurrences.id -> normalized_occurrences.read.where(s"chromosome='$chromosome'")
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {

    data(normalized_occurrences.id)
      .where(col("zygosity").isin("HOM", "HET"))
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }
}