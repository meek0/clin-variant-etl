package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.GenomicsUtils._
import bio.ferlab.clin.etl.utils.VcfUtils.columns.{formatted_consequences, _}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

object Consequences {

  def run(input: String, output: String, lastExecutionDateTime: Timestamp)(implicit spark: SparkSession): Unit = {
    val inputDf = spark.table("clin_raw.consequences").where(col("updatedOn") >= lastExecutionDateTime)
    val outputDf = build(inputDf)

    DeltaUtils.upsert(
      outputDf,
      Some(output),
      "clin",
      "consequences",
      {
        _.repartition(1, col("chromosome")).sortWithinPartitions("start")
      },
      locusColumnNames :+ "ensembl_gene_id" :+ "ensembl_feature_id",
      Seq("chromosome")
    )
  }

  def build(csq: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val df = csq
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_transcript_id", "ensembl_regulatory_id")
      .withColumn("consequence", formatted_consequences)

    joinWithDBNSFP(df)
  }

  def joinWithDBNSFP(csq: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dbnsfp = spark.table("clin.dbnsfp_original")
      .withColumn("start", col("start").cast(LongType))
      .selectLocus(
        $"ensembl_transcript_id" as "ensembl_feature_id",
        struct(
          $"SIFT_converted_rankscore" as "sift_converted_rank_score",
          $"SIFT_pred" as "sift_pred",
          $"Polyphen2_HVAR_rankscore" as "polyphen2_hvar_score",
          $"Polyphen2_HVAR_pred" as "polyphen2_hvar_pred",
          $"FATHMM_converted_rankscore",
          $"FATHMM_pred" as "fathmm_pred",
          $"CADD_raw_rankscore" as "cadd_score",
          $"DANN_rankscore" as "dann_score",
          $"REVEL_rankscore" as "revel_rankscore",
          $"LRT_converted_rankscore" as "lrt_converted_rankscore",
          $"LRT_pred" as "lrt_pred") as "predictions",
        struct($"phyloP17way_primate_rankscore" as "phylo_p17way_primate_rankscore") as "conservations",
      )

    csq
      .join(dbnsfp, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(csq("*"), dbnsfp("predictions"), dbnsfp("conservations"))

  }

}
