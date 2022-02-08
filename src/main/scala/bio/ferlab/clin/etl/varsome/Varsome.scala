package bio.ferlab.clin.etl.varsome

import bio.ferlab.clin.etl.varsome.VarsomeUtils.{tableExist, transformPartition, varsomeSchema}
import bio.ferlab.clin.etl.vcf.{ForBatch, Reload, VarsomeJobType}
import bio.ferlab.datalake.commons.config.RunStep.reset
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

object Varsome extends SparkApp {

  val varsomeToken = sys.env("varsome_token")
  val varsomeUrl = sys.env("varsome_url")

  implicit val (conf, steps, spark) = init()
  if (steps.contains(reset)) {
    new Varsome(Reload, varsomeUrl, varsomeToken).run()
  } else {
    if (args.length < 3) {
      throw new IllegalArgumentException("Batch id is required if reset is not included in steps")
    }
    val batchId = args(2)
    new Varsome(ForBatch(batchId), varsomeUrl, varsomeToken).run()
  }

  spark.sparkContext.setLogLevel("ERROR")
}


class Varsome(jobType: VarsomeJobType, varsomeUrl: String, varsomeToken: String)(override implicit val conf: Configuration) extends ETL {
  override val destination: DatasetConf = conf.getDataset("normalized_varsome")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    val variants = normalized_variants.read
      .select("chromosome", "start", "reference", "alternate")
    jobType match {
      case Reload => Map(normalized_variants.id -> variants)
      case ForBatch(batchId) =>
        val batchVariants = variants
          .where(col("batch_id") === batchId)
        val varsomeExist: Boolean = tableExist(destination)
        val extractedVariants = if (varsomeExist) {
          val varsome = destination.read.where(col("updated_on") >= Timestamp.valueOf(currentRunDateTime.minusDays(7)))
          batchVariants.join(varsome, Seq("chromosome", "start", "reference", "alternate"), "leftanti")
        }
        else {
          batchVariants
        }
        Map(normalized_variants.id -> extractedVariants)
    }


  }


  override def transform(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val input = data(normalized_variants.id)
    val numPartitions = (input.count() / 1000) + 1

    input.select(concat_ws("-", locus: _*) as "locus")
      .repartition(numPartitions.toInt)
      .mapPartitions(transformPartition(varsomeUrl, varsomeToken))
      .select(from_json(col("response"), varsomeSchema) as "response")
      .select(explode(col("response")) as "response")
      .select("response.*")
      .withColumn("chromosome", ltrim(col("chromosome"), "chr"))
      .withColumnRenamed("pos", "start")
      .withColumnRenamed("ref", "reference")
      .withColumnRenamed("alt", "alternate")
      .withColumn("updated_on", lit(Timestamp.valueOf(currentRunDateTime)))
      .repartition(25)


  }


}

