package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.utils.GenomicsUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PrepareIndex extends App {

  val Array(output, batchId) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Prepare Index").getOrCreate()

  run(output, batchId)
  runUpdate(output, batchId)

  def run(output: String, lastExecutionDateTime: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val newVariants =
      spark.table("clin.variants")
        .where(col("createdOn") >= lastExecutionDateTime)

    val finalDf = buildVariants(newVariants)
    finalDf
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/extract")
    finalDf
  }

  def runUpdate(output: String, lastExecutionDateTime: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val updatedVariants =
      spark.table("clin.variants")
        .where(col("updatedOn") >= lastExecutionDateTime and col("createdOn") =!= col("updatedOn"))

    val finalDf = buildVariants(updatedVariants)
      .withColumn("frequencies", map(lit("internal"), col("frequencies.internal")))
      .select("chromosome", "start", "reference", "alternate", "donors", "lab_frequencies", "frequencies", "participant_number")

    finalDf
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/update")
    finalDf
  }

  private def buildVariants(variantDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val consequences = spark.table("clin.consequences").as("consequences")

    variantDF
      .joinByLocus(consequences, "inner")
      .groupByLocus()
      .agg(
        first(struct("variants.*")) as "variant",
        collect_list(struct("consequences.*")) as "consequences",
        max("impact_score") as "impact_score")
      .select($"variant.*", $"consequences", $"impact_score")
  }

}
