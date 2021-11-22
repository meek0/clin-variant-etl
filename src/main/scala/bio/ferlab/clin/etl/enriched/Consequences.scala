package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.formatted_consequences
import org.apache.spark.sql.functions.{coalesce, col, lit, struct}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class Consequences(chromosome: String, loadType: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("normalized_dbnsfp_original")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    loadType match {
      case "first_load" =>
        FileSystemResolver.resolve(conf.getStorage(destination.storageid).filesystem).remove(destination.location)
        destination.table.foreach(t => spark.sql(s"DROP TABLE IF EXISTS ${t.fullName}"))
        run(minDateTime, LocalDateTime.now())
      case _ =>
        run(minDateTime, LocalDateTime.now())
    }
  }

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_consequences.id -> normalized_consequences.read
        .where(col("updated_on") >= Timestamp.valueOf(lastRunDateTime)).where(s"chromosome='$chromosome'")
      ,
      dbnsfp_original.id -> dbnsfp_original.read,
      normalized_ensembl_mapping.id -> normalized_ensembl_mapping.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequences = data(normalized_consequences.id)

    val ensembl_mapping = data(normalized_ensembl_mapping.id)

    val chromosomes = consequences.select("chromosome").distinct().as[String].collect()

    val dbnsfp = data(dbnsfp_original.id).where(col("chromosome").isin(chromosomes:_*))

    val csq = consequences
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_regulatory_id")
      .withColumn("consequence", formatted_consequences)
      .withColumnRenamed("impact", "vep_impact")

    val csqWithDBNSF = joinWithDBNSFP(csq, dbnsfp)

    joinWithEnsemblMapping(csqWithDBNSF, ensembl_mapping)
      .withColumnRenamed("is_canonical", "canonical")
      .withColumnRenamed("is_mane_select", "mane_select")
      .withColumnRenamed("is_mane_plus", "mane_plus")
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }

  def joinWithEnsemblMapping(targetDf: DataFrame, ensembl_mapping: DataFrame): DataFrame = {
    targetDf
      .join(ensembl_mapping, Seq("ensembl_transcript_id", "ensembl_gene_id"), "left")
      .withColumn("canonical", coalesce(col("is_canonical"), lit(false)))
      .drop("is_canonical")
  }

  def joinWithDBNSFP(csq: DataFrame, dbnsfp: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dbnsfpRenamed =
      dbnsfp
        .withColumn("start", col("start").cast(LongType))
        .selectLocus(
          $"ensembl_transcript_id" as "ensembl_feature_id",
          struct(
            $"SIFT_converted_rankscore" as "sift_converted_rank_score",
            $"SIFT_pred" as "sift_pred",
            $"Polyphen2_HVAR_rankscore" as "polyphen2_hvar_score",
            $"Polyphen2_HVAR_pred" as "polyphen2_hvar_pred",
            $"FATHMM_converted_rankscore" as "fathmm_converted_rankscore",
            $"FATHMM_pred" as "fathmm_pred",
            $"CADD_raw_rankscore" as "cadd_score",
            $"DANN_rankscore" as "dann_score",
            $"REVEL_rankscore" as "revel_rankscore",
            $"LRT_converted_rankscore" as "lrt_converted_rankscore",
            $"LRT_pred" as "lrt_pred") as "predictions",
          struct($"phyloP17way_primate_rankscore" as "phylo_p17way_primate_rankscore") as "conservations",
        )

    csq
      .join(dbnsfpRenamed, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(csq("*"), dbnsfpRenamed("predictions"), dbnsfpRenamed("conservations"))
      .withColumn(destination.oid, col("created_on"))

  }
}
