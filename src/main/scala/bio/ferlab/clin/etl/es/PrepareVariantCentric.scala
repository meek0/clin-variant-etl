package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class PrepareVariantCentric(releaseId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("es_index_variant_centric")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      enriched_variants.id -> enriched_variants.read,
      enriched_consequences.id -> enriched_consequences.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val variants = data(enriched_variants.id)
      .drop("transmissions", "transmissions_by_lab", "parental_origins", "parental_origins_by_lab",
        "normalized_variants_oid", "variants_oid", "created_on", "updated_on")
      .as("variants")

    val variantsWithFrequencies = variants
      .select(col("variants.*"), explode(col("frequencies_by_lab")).as(List("lab_name", "fbl")))
      .withColumn("frequencies_by_lab", struct(
        col("lab_name") ,
        col("fbl")("an") as "an",
        col("fbl")("ac") as "ac",
        col("fbl")("af") as "af",
        col("fbl")("hom") as "hom",
        col("fbl")("het") as "het"
      ))
      .groupByLocus()
      .agg(
        collect_list("frequencies_by_lab") as "frequencies_by_lab",
        variants.drop("frequencies_by_lab", "chromosome", "start", "reference", "alternate").columns.map(c => first(c) as c):_*
      ).as("variants")

    val consequences = data(enriched_consequences.id)
      .drop("normalized_consequences_oid", "consequences_oid", "created_on", "updated_on")
      .as("consequences")

    joinWithConsequences(variantsWithFrequencies, consequences)
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val path = s"${destination.rootPath}${destination.path}_${releaseId}"
    data
      //avoids many small files created by the following partitionBy() operation
      .repartition(10, col("chromosome"))
      .write
      .option("maxRecordsPerFile", 200000)
      .partitionBy(destination.partitionby:_*)
      .mode(SaveMode.Overwrite)
      .option("format", destination.format.sparkFormat)
      .option("path", path)
      .saveAsTable(s"${destination.table.get.fullName}_${releaseId}")
    println(s"SAVING data on: ${path}")
    data
  }

  private def joinWithConsequences(variantDF: DataFrame, consequencesDf: DataFrame)
                                  (implicit spark: SparkSession): DataFrame = {
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

  private def getUpdate(consequencesDf: DataFrame,
                        variantsDf: DataFrame,
                        lastExecution: Timestamp)
                       (implicit spark: SparkSession): DataFrame = {
    val updatedVariants = variantsDf
      .where(col("updated_on") >= lastExecution and col("created_on") =!= col("updated_on"))
      .drop("created_on", "updated_on")

    joinWithConsequences(updatedVariants, consequencesDf)
      .withColumn("frequencies", map(lit("internal"), col("frequencies.internal")))
      .select("chromosome", "start", "reference", "alternate", "donors", "frequencies_by_lab", "frequencies",
        "participant_number")
  }
}

