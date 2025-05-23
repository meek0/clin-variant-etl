package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.formatted_consequences
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.sql.Timestamp
import java.time.LocalDateTime

case class Consequences(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_consequences.id -> normalized_consequences.read
        .where(col("updated_on") >= Timestamp.valueOf(lastRunDateTime)),
      dbnsfp_original.id -> dbnsfp_original.read,
      normalized_ensembl_mapping.id -> normalized_ensembl_mapping.read,
      enriched_genes.id -> enriched_genes.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val consequences = data(normalized_consequences.id)

    val ensembl_mapping = data(normalized_ensembl_mapping.id)
      .withColumn("uniprot_id", col("uniprot")(0)("id"))
      .select(
        $"ensembl_transcript_id",
        $"ensembl_gene_id",
        $"uniprot_id",
        //$"refseq_mrna_id",
        //$"refseq_protein_id",
        $"is_mane_select" as "mane_select",
        $"is_mane_plus" as "mane_plus",
        $"is_canonical")

    val chromosomes = consequences.select("chromosome").distinct().as[String].collect()

    val dbnsfp = data(dbnsfp_original.id).where(col("chromosome").isin(chromosomes: _*))

    val csq = consequences
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_regulatory_id")
      .withColumn("consequence", formatted_consequences)
      .withColumnRenamed("impact", "vep_impact")
      .withColumn("aa_change",
        when(col("hgvsp").isNotNull && col("hgvsp").contains(":"),
        split(col("hgvsp"), ":").getItem(1))
        .otherwise(lit(null).cast("string")))

    joinWithDBNSFP(csq, dbnsfp)
      .join(ensembl_mapping, Seq("ensembl_transcript_id", "ensembl_gene_id"), "left")
      //.join(mane_summary, Seq("ensembl_transcript_id", "ensembl_gene_id"), "left")
      .withColumn("mane_plus", coalesce(col("mane_plus"), lit(false)))
      .withColumn("mane_select", coalesce(col("mane_select"), lit(false)))
      .withColumn("canonical", coalesce(col("is_canonical"), lit(false)))
      .drop("is_canonical")
      .withPickedCsqPerLocus(data(enriched_genes.id))
  }

  override def publish(): Unit = {
    compact(mainDestination, RepartitionByColumns(Seq("chromosome"), Some(100), Seq("start")))
    vacuum(mainDestination, 2)
  }

  def joinWithDBNSFP(csq: DataFrame, dbnsfp: DataFrame): DataFrame = {
    val dbnsfpRenamed =
      dbnsfp
        .filter($"aaref".isNotNull)
        .withColumn("start", col("start").cast(LongType))
        .selectLocus(
          $"ensembl_transcript_id" as "ensembl_feature_id",
          struct(
            $"SIFT_score" as "sift_score",
            $"SIFT_pred" as "sift_pred",
            $"Polyphen2_HVAR_score" as "polyphen2_hvar_score",
            $"Polyphen2_HVAR_pred" as "polyphen2_hvar_pred",
            $"FATHMM_score" as "fathmm_score",
            $"FATHMM_pred" as "fathmm_pred",
            $"CADD_raw" as "cadd_score",
            $"CADD_phred" as "cadd_phred",
            $"DANN_score" as "dann_score",
            $"REVEL_score" as "revel_score",
            $"LRT_score" as "lrt_score",
            $"phyloP100way_vertebrate" as "phyloP100way_vertebrate",
            $"LRT_pred" as "lrt_pred") as "predictions",
          struct($"phyloP17way_primate" as "phylo_p17way_primate_score") as "conservations",
        )

    csq
      .join(dbnsfpRenamed, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(csq("*"), dbnsfpRenamed("predictions"), dbnsfpRenamed("conservations"))
      .withColumn(mainDestination.oid, col("created_on"))

  }
}

object Consequences {
  implicit class DataFrameOps(df: DataFrame) {
    def pickRandomCsqPerVariant: DataFrame = {
      df
        .groupByLocus()
        .agg(first("ensembl_transcript_id") as "ensembl_transcript_id")
    }
  }

  @main
  def run(rc: RuntimeETLContext): Unit = {
    Consequences(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
