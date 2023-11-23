package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.mainutils.Release
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class PrepareGeneCentric(rc: DeprecatedRuntimeETLContext, releaseId: String) extends PrepareCentric(rc, releaseId) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_gene_centric")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      enriched_genes.id -> spark.table(s"${enriched_genes.table.get.fullName}"),
      enriched_variants.id -> enriched_variants.read,
      enriched_cnv.id -> enriched_cnv.read,
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val variants = data(enriched_variants.id)
      .select(explode($"donors.patient_id") as "patient_id", $"genes_symbol", $"locus")
      .select($"patient_id", explode($"genes_symbol") as "symbol", $"locus")
      .dropDuplicates
      .groupBy("patient_id", "symbol")
      .agg(count($"locus") as "number_of_snvs_per_patient")
      .groupBy("symbol")
      .agg(
        count($"patient_id") as "number_of_patients_snvs",
        collect_list(struct(
          $"patient_id",
          $"number_of_snvs_per_patient" as "count"
        )) as "number_of_snvs_per_patient"
      )

    val cnvs = data(enriched_cnv.id) // locus doest exist in CNV, build it
      .withColumn("locus", concat(locus.head, lit("-"), locus(1), lit("-"), locus(2), lit("-"), locus(3)))
      .select(explode($"genes") as "genes", $"patient_id", $"locus")
      .select("patient_id", "genes.symbol", "locus")
      .dropDuplicates
      .groupBy("patient_id", "symbol")
      .agg(count($"locus") as "number_of_cnvs_per_patient")
      .groupBy("symbol")
      .agg(
        count($"patient_id") as "number_of_patients_cnvs",
        collect_list(struct(
          $"patient_id",
          $"number_of_cnvs_per_patient" as "count"
        )) as "number_of_cnvs_per_patient"
      )

    val all = variants.join(cnvs, Seq("symbol"), "full")

    data(enriched_genes.id)
      .join(all, Seq("symbol"), "full")
      .withColumn("hash", sha1(col("symbol")))
      .withColumn("entrez_gene_id", coalesce(col("entrez_gene_id"), lit(0)))
      .withColumn("alias", coalesce(col("alias"), lit(array())))
      .withColumn("number_of_patients_snvs", coalesce(col("number_of_patients_snvs"), lit(0)))
      .withColumn("number_of_patients_cnvs", coalesce(col("number_of_patients_cnvs"), lit(0)))
      .withColumn("number_of_snvs_per_patient", coalesce(col("number_of_snvs_per_patient"), lit(array())))
      .withColumn("number_of_cnvs_per_patient", coalesce(col("number_of_cnvs_per_patient"), lit(array())))
  }

}

object PrepareGeneCentric {
  @main
  def run(rc: DeprecatedRuntimeETLContext, release: Release): Unit = {
    PrepareGeneCentric(rc, release.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
