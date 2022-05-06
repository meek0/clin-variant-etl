package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{array, coalesce, col, collect_list, count, explode, lit, sha1, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._

import java.time.LocalDateTime

class PrepareGeneCentric(releaseId: String)
                        (override implicit val conf: Configuration) extends ETL() {

  override val destination: DatasetConf = conf.getDataset("es_index_gene_centric")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      enriched_genes.id -> spark.table(s"${enriched_genes.table.get.fullName}"),
      enriched_variants.id -> enriched_variants.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val variants = data(enriched_variants.id)
      .select(explode($"donors.patient_id") as "patient_id", $"genes_symbol", $"locus")
      .select($"patient_id", explode($"genes_symbol") as "symbol", $"locus")
      .dropDuplicates
      .groupBy("patient_id", "symbol")
      .agg(count($"locus") as "number_of_variants_per_patient")
      .groupBy("symbol")
      .agg(
        count($"patient_id") as "number_of_patients",
        collect_list(struct(
          $"patient_id",
          $"number_of_variants_per_patient" as "count"
        )) as "number_of_variants_per_patient"
      )

    data(enriched_genes.id)
      .join(variants, Seq("symbol"), "full")
      .withColumn("hash", sha1(col("symbol")))
      .withColumn("entrez_gene_id", coalesce(col("entrez_gene_id"), lit(0)))
      .withColumn("alias", coalesce(col("alias"), lit(array())))
      .withColumn("number_of_patients", coalesce(col("number_of_patients"), lit(0)))
      .withColumn("number_of_variants_per_patient", coalesce(col("number_of_variants_per_patient"), lit(array())))
    
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame =
    loadForReleaseId(data, destination, releaseId)
}

