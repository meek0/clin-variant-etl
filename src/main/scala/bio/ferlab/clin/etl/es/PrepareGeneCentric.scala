package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.locusColumnNames
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class PrepareGeneCentric(releaseId: String)
                        (override implicit val conf: Configuration) extends PrepareCentric(releaseId) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_gene_centric")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      enriched_genes.id -> spark.table(s"${enriched_genes.table.get.fullName}"),
      enriched_variants.id -> enriched_variants.read,
      enriched_cnv.id -> enriched_cnv.read,
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val variants = data(enriched_variants.id)
      .select(explode($"donors.patient_id") as "patient_id", $"genes_symbol", $"locus")
      .select($"patient_id", explode($"genes_symbol") as "symbol", $"locus")
      .dropDuplicates

    val cnvs = data(enriched_cnv.id) // locus doest exist in CNV, build it
      .withColumn("locus", concat(locus(0), lit("-"), locus(1), lit("-"), locus(2), lit("-"), locus(3)))
      .select(explode($"genes") as "genes", $"patient_id", $"locus")
      .select("patient_id", "genes.symbol", "locus")
      .dropDuplicates

    val groupBySymbol = variants.union(cnvs).groupBy("patient_id", "symbol")
      .agg(count($"locus") as "number_of_variants_per_patient")
      .groupBy("symbol")
      .agg(
        count($"patient_id") as "number_of_patients",
        collect_list(struct(
          $"patient_id",
          $"number_of_variants_per_patient" as "count"
        )) as "number_of_variants_per_patient"
      )

    variants.union(cnvs).show(100, false)
    groupBySymbol.show(100, false)

    data(enriched_genes.id)
      .join(groupBySymbol, Seq("symbol"), "full")
      .withColumn("hash", sha1(col("symbol")))
      .withColumn("entrez_gene_id", coalesce(col("entrez_gene_id"), lit(0)))
      .withColumn("alias", coalesce(col("alias"), lit(array())))
      .withColumn("number_of_patients", coalesce(col("number_of_patients"), lit(0)))
      .withColumn("number_of_variants_per_patient", coalesce(col("number_of_variants_per_patient"), lit(array())))
  }

}

