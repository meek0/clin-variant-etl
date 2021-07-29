package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.utils.GenomicsUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PrepareVariantIndex extends App {

  val Array(output, lastBatch) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Prepare Index").getOrCreate()

  //val minimumDateTime = LocalDateTime.of(1900, 1 , 1, 0, 0, 0)
  //val lastExecutionTimestamp = Try(Timestamp.valueOf(lastExecutionString)).getOrElse(Timestamp.valueOf(minimumDateTime))

  run(output, lastBatch)
  runUpdate(output, lastBatch)

  def run(output: String, lastExecution: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val newVariants =
      spark.table("clin.variants")
        .where(col("createdOn") >= lastExecution)

    val finalDf = buildVariants(newVariants)
    finalDf
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/extract")
    finalDf
  }

  def runUpdate(output: String, lastExecution: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql("use clin")

    val updatedVariants =
      spark.table("clin.variants")
        .where(col("updatedOn") >= lastExecution and col("createdOn") =!= col("updatedOn"))

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
