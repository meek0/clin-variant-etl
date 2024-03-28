package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}

import java.time.LocalDateTime

case class PrepareVariantSuggestions(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_variant_suggestions")
  val es_index_variant_centric: DatasetConf = conf.getDataset("es_index_variant_centric")

  final val high_priority_weight = 4
  final val low_priority_weight = 2

  final val indexColumns =
    List("type", "locus", "suggestion_id", "hgvsg", "suggest", "chromosome", "rsnumber", "symbol_aa_change")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(
      es_index_variant_centric.id -> es_index_variant_centric.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    val variants = data(es_index_variant_centric.id)

    variants
      .select(
        lit("variant") as "type",
        $"chromosome",
        $"locus",
        $"hash" as "suggestion_id",
        $"hgvsg",
        $"rsnumber",
        $"consequences",
        $"clinvar.clinvar_id" as "clinvar_id"
      )
      .withColumn("ensembl_gene_ids", col("consequences.ensembl_gene_id"))
      .withColumn("ensembl_feature_ids", col("consequences.ensembl_feature_id"))
      .withColumn("symbols", col("consequences.symbol"))
      .withColumn("symbol_aa_change",
        array_distinct(
          array_remove(functions.transform(col("consequences"), c => concat_ws(" ", c("symbol"), c("aa_change"))), "")))
      .withColumn("suggest", array(
        struct(
          lit(high_priority_weight) as "weight",
          array_distinct(array_remove(functions.transform(
            array(col("hgvsg"), col("rsnumber"), col("locus"), col("clinvar_id")),
            c => when(c.isNull, lit("")).otherwise(c)), ""))
            as "input"
        ),
        struct(
          lit(low_priority_weight) as "weight",
          array_distinct(array_remove(functions.transform(
            array_union(col("symbols"),
              array_union(col("ensembl_feature_ids"),
                array_union(col("symbol_aa_change"), col("ensembl_gene_ids")))),
            c => when(c.isNull, lit("")).otherwise(c)), ""))
            as "input"
        )
      ))
      .drop("consequences", "ensembl_gene_ids", "ensembl_feature_ids", "symbols")
  }

}

object PrepareVariantSuggestions {
  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    PrepareVariantSuggestions(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
