package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

object VariantIndex {

  def getInsert(lastExecution: Timestamp)
               (implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val enriched_variants = conf.getDataset("enriched_variants")
    val enriched_consequences = conf.getDataset("enriched_consequences")

    val newVariants = spark
      .read
      .format(enriched_variants.format.sparkFormat)
      .load(enriched_variants.location)
      .where(col("created_on") >= lastExecution)
      .drop("transmissions", "transmissions_by_lab", "parental_origins", "parental_origins_by_lab")
      .as("variants")

    val consequences =
      spark
        .read
        .format(enriched_consequences.format.sparkFormat)
        .load(enriched_consequences.location)
        .as("consequences")

    joinWithConsequences(newVariants, consequences)
  }

  def getUpdate(lastExecution: Timestamp)
               (implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val enriched_variants = conf.getDataset("enriched_variants")
    val enriched_consequences = conf.getDataset("enriched_consequences")

    val updatedVariants =
      spark
        .read
        .format(enriched_variants.format.sparkFormat)
        .load(enriched_variants.location)
        .where(col("updated_on") >= lastExecution and col("created_on") =!= col("updated_on"))
        .as("variants")

    val consequences =
      spark
        .read
        .format(enriched_consequences.format.sparkFormat)
        .load(enriched_consequences.location)
        .as("consequences")

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
        max("impact_score") as "impact_score")
      .select($"variant.*", $"consequences", $"impact_score")
  }

}
