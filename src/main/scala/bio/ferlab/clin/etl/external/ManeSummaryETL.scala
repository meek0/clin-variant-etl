package bio.ferlab.clin.etl.external

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.time.LocalDateTime

class ManeSummaryETL()(override implicit val conf: Configuration) extends ETL {
  override val mainDestination: DatasetConf = conf.getDataset("normalized_mane_summary")
  val raw_mane_summary: DatasetConf = conf.getDataset("raw_mane_summary")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)
                      (implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_mane_summary.id -> raw_mane_summary.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime,
                         currentRunDateTime: LocalDateTime)
                        (implicit spark: SparkSession): Map[String, DataFrame] = {
    val mane_summaryDf = data(raw_mane_summary.id)
      .withColumnRenamed("Ensembl_Gene", "ensembl_gene_id")
      .withColumnRenamed("Ensembl_nuc", "ensembl_transcript_id")
      .withColumnRenamed("Ensembl_prot", "ensembl_feature_id")
      .withColumnRenamed("RefSeq_nuc", "refseq_mrna_id")
      .withColumnRenamed("RefSeq_prot", "refseq_protein_id")
      .withColumnRenamed("#NCBI_GeneID", "NCBI_gene_id")
      .withColumnRenamed("HGNC_ID", "HGNC_id")
      .withColumn("chr_start", col("chr_start").cast(LongType))
      .withColumn("chr_end", col("chr_end").cast(LongType))
      .withColumn("mane_select", when(col("MANE_status") === "MANE Select", lit(true)).otherwise(lit(false)))
      .withColumn("mane_plus", when(col("MANE_status") === "MANE Plus Clinical", lit(true)).otherwise(lit(false)))
    Map(mainDestination.id -> mane_summaryDf.repartition(1))
  }

}
