package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}

import java.time.LocalDateTime

case class PrepareGeneSuggestions(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_gene_suggestions")
  val es_index_gene_centric: DatasetConf = conf.getDataset("es_index_gene_centric")

  final val high_priority_weight = 4
  final val low_priority_weight  = 2

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(
      es_index_gene_centric.id -> es_index_gene_centric.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    val genes = data(es_index_gene_centric.id)

    genes
      .select(
        lit("gene") as "type",
        $"chromosome",
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

}

object PrepareGeneSuggestions {
  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    PrepareGeneSuggestions(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
