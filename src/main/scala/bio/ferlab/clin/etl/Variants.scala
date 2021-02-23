package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.columns._
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object Variants {

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    val inputDF = vcf(input)
    val annotations: DataFrame = build(inputDF, batchId)

    write(annotations, output)

    //    val deltaTable = DeltaTable.forName("variants")
    //    deltaTable.vacuum()

  }

  private def write(variants: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    Try(DeltaTable.forName("variants")) match {
      case Failure(_) => writeOnce(variants, output)
      case Success(existingVariants) =>
        /** Merge */
        existingVariants.as("e")
          .merge(
            variants.as("v"),
            variants("chromosome") === $"e.chromosome" &&
              variants("start") === $"e.start" &&
              variants("reference") === $"e.reference" &&
              variants("alternate") === $"e.alternate"
          )
          .whenMatched()
          .updateExpr(Map("last_batch_id" -> "v.batch_id"))
          .whenNotMatched()
          .insertAll()
          .execute()

        /** Compact */
        writeOnce(spark.table("variants"), output, dataChange = false)
    }
  }

  private def writeOnce(df: DataFrame, output: String, dataChange: Boolean = true)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    df
      .repartition(1, $"chromosome")
      .sortWithinPartitions("start")
      .write.option("dataChange", dataChange)
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("delta")
      .option("path", s"$output/variants")
      .saveAsTable("variants")
  }

  def build(inputDF: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variants = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        is_multi_allelic,
        old_multi_allelic,
        firstAnn,
        array_distinct(annotations("symbol")) as "genes_symbol",
        hgvsg,
        variant_class,
        pubmed,
        lit(batchId) as "batch_id",
        lit(null).cast("string") as "last_batch_id"
      )
      .drop("annotation")

    variants
  }
}
