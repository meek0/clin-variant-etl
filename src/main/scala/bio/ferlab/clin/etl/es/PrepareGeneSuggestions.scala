package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class PrepareGeneSuggestions(releaseId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("es_index_gene_suggestions")
  val es_index_gene_centric: DatasetConf = conf.getDataset("es_index_gene_centric")

  final val high_priority_weight = 4
  final val low_priority_weight  = 2

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      es_index_gene_centric.id ->
        es_index_gene_centric
          .copy(table = es_index_gene_centric.table.map(t => t.copy(name = s"${t.name}_$releaseId")))
          .read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val genes = data(es_index_gene_centric.id)

    genes
      .select(
        lit("gene") as "type",
        $"symbol",
        $"ensembl_gene_id",
        $"hash" as "suggestion_id",
        $"alias"
      )
      .withColumn("suggest", array(
        struct(
          lit(high_priority_weight) as "weight",
          array_distinct(array_remove(functions.transform(
            array(col("symbol")),
            c => when(c.isNull, lit("")).otherwise(c)), ""))
            as "input"
        ),
        struct(
          lit(low_priority_weight) as "weight",
          array_distinct(array_remove(functions.transform(
            array_union(col("alias"), array(col("ensembl_gene_id"))),
            c => when(c.isNull, lit("")).otherwise(c)), ""))
            as "input"
        )
      ))
      .drop("alias")
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame =
    loadForReleaseId(data, destination, releaseId)
}

