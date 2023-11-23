package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.SNV.transformSingleSNV
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

case class SNV(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_snv")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val normalized_exomiser: DatasetConf = conf.getDataset("normalized_exomiser")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_snv.id -> normalized_snv.read,
      normalized_exomiser.id -> normalized_exomiser.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
   transformSingleSNV(data(normalized_snv.id), data(normalized_exomiser.id))
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(1), sortColumns = Seq("start"))
}

object SNV {
  def transformSingleSNV(snv: DataFrame, exomiser: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val exo = exomiser.selectLocus(
      $"aliquot_id",
      $"exomiser_variant_score",
      $"contributing_variant",
      struct(
        "gene_combined_score", // put first since sort_array uses first numeric field to sort an array of struct
        "rank",
        "gene_symbol",
        "moi",
        "acmg_classification",
        "acmg_evidence"
      ) as "exomiser_struct"
    )
      .groupBy(locus :+ $"aliquot_id": _*)
      .agg(
        max($"exomiser_variant_score") as "exomiser_variant_score",
        sort_array(collect_list(when($"contributing_variant", $"exomiser_struct")), asc = false) as "exomiser_struct_list", // sort by gene_combined_score in desc order
      )
      .withColumn("exomiser", $"exomiser_struct_list".getItem(0))
      .withColumn("exomiser_other_moi", $"exomiser_struct_list".getItem(1))
      .selectLocus(
        $"aliquot_id",
        $"exomiser_variant_score",
        $"exomiser",
        $"exomiser_other_moi",
      )

    snv.join(exo, locusColumnNames :+ "aliquot_id", "left")
  }

  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    SNV(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
