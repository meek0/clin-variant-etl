package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext, RepartitionByColumns}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.clin.etl.enriched.SNV._
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
  val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_snv.id -> normalized_snv.read,
      normalized_exomiser.id -> normalized_exomiser.read,
      normalized_franklin.id -> normalized_franklin.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(normalized_snv.id)
      .withExomiser(data(normalized_exomiser.id))
      .withFranklin(data(normalized_franklin.id))
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(1), sortColumns = Seq("start"))
}

object SNV {
  implicit class DataFrameOps(df: DataFrame) {
    def withExomiser(exomiser: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val exomiserPrepared = exomiser.selectLocus(
        $"aliquot_id",
        $"contributing_variant",
        struct(
          $"gene_combined_score", // put first since sort_array uses first numeric field to sort an array of struct
          $"rank",
          $"exomiser_variant_score" as "variant_score",
          $"gene_symbol",
          $"moi",
          $"acmg_classification",
          $"acmg_evidence"
        ) as "exomiser_struct"
      )
        .groupBy(locus :+ $"aliquot_id": _*)
        .agg(
          sort_array(collect_list(when($"contributing_variant", $"exomiser_struct")), asc = false) as "exomiser_struct_list", // sort by gene_combined_score in desc order
        )
        .withColumn("exomiser", $"exomiser_struct_list".getItem(0))
        .withColumn("exomiser_other_moi", $"exomiser_struct_list".getItem(1).dropFields("variant_score")) // variant_score should only be in exomiser struct
        .selectLocus(
          $"aliquot_id",
          $"exomiser",
          $"exomiser_other_moi",
        )

      df.join(exomiserPrepared, locusColumnNames :+ "aliquot_id", "left")
    }

    def withFranklin(franklin: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val preparedFranklin = franklin
        .where($"aliquot_id".isNotNull) // Remove family analyses
        .selectLocus(
          $"aliquot_id",
          $"score"
        )
        .groupByLocus($"aliquot_id")
        .agg(first("score", ignoreNulls = true) as "franklin_combined_score") // There should only be one franklin score per locus+aliquot

      df.join(preparedFranklin, locusColumnNames :+ "aliquot_id", "left")
    }
  }

  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    SNV(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
