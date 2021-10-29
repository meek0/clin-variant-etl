package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

object VariantIndex {

  def consequencesDf(implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val enriched_consequences = conf.getDataset("enriched_consequences")
    spark
      .read
      .format(enriched_consequences.format.sparkFormat)
      .load(enriched_consequences.location)
      .drop("normalized_consequences_oid", "consequences_oid", "created_on", "updated_on")
      .as("consequences")
  }

  def variantsDf(implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val enriched_variants = conf.getDataset("enriched_variants")

    spark
      .read
      .format(enriched_variants.format.sparkFormat)
      .load(enriched_variants.location)
      .drop("transmissions", "transmissions_by_lab", "parental_origins", "parental_origins_by_lab",
        "normalized_variants_oid", "variants_oid")
      .as("variants")
  }


  def getInsert(lastExecution: Timestamp)
               (implicit spark: SparkSession, conf: Configuration): DataFrame = {

    val consequences = consequencesDf
    val newVariants = variantsDf
      .where(col("created_on") >= lastExecution)
      .drop("created_on", "updated_on")

    joinWithConsequences(newVariants, consequences)
  }

  def getUpdate(lastExecution: Timestamp)
               (implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val consequences = consequencesDf
    val updatedVariants = variantsDf
      .where(col("updated_on") >= lastExecution and col("created_on") =!= col("updated_on"))
      .drop("created_on", "updated_on")

    val finalDf = joinWithConsequences(updatedVariants, consequences)
      .withColumn("frequencies", map(lit("internal"), col("frequencies.internal")))
      .select("chromosome", "start", "reference", "alternate", "donors", "frequencies_by_lab", "frequencies",
        "participant_number")

    finalDf
  }

  private def joinWithConsequences(variantDF: DataFrame, consequencesDf: DataFrame)
                                  (implicit spark: SparkSession, conf: Configuration): DataFrame = {
    import spark.implicits._

    variantDF
      .joinByLocus(consequencesDf, "left")
      .groupByLocus()
      .agg(
        first(struct("variants.*")) as "variant",
        collect_list(struct("consequences.*")) as "consequences",
        max("impact_score") as "max_impact_score")
      .select($"variant.*", $"consequences", $"max_impact_score")
  }

}
